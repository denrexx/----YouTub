import hashlib
import io
import json
import os
import random
import re
import sqlite3
import subprocess
import threading
import time
import fcntl
from pathlib import Path

import telebot
from gtts import gTTS
from telebot.types import InlineKeyboardButton, InlineKeyboardMarkup

from config import TOKEN


BASE_DIR = Path(__file__).resolve().parent
WORDS_FILE = BASE_DIR / "5000.txt"
PHRASAL_FILE = BASE_DIR / "phrasal_verbs_100.txt"
STATE_FILE = BASE_DIR / "progress.json"
DATABASE_FILE = BASE_DIR / "mistakes.db"
AUDIO_DIR = BASE_DIR / "audio"
ONLINE_REQUEST_DELAY = 4

bot = telebot.TeleBot(TOKEN, threaded=True, num_threads=4)
STATE_LOCK = threading.RLock()
CHAT_LOCKS = {}
VOICE_LOCK = threading.Lock()
VOICE_JOBS = {}
VOICE_SENDS = set()
HOST_PLAYER_LOCK = threading.Lock()
HOST_PLAYER = None


def load_words(path):
    words = []
    for expected_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        number, value = line.split(". ", 1)
        word, translation = value.split(" - ", 1)
        if int(number) != expected_number or not word or not translation:
            raise ValueError(f"Invalid entry {expected_number} in {path}")
        words.append({"number": int(number), "word": word, "translation": translation})
    return words


WORDS = load_words(WORDS_FILE)
PHRASAL_VERBS = load_words(PHRASAL_FILE)
WORD_INDEX_BY_WORD = {item["word"]: index for index, item in enumerate(WORDS)}
WORD_MODES = {"level", "test", "custom", "mistakes"}
if len(WORD_INDEX_BY_WORD) != len(WORDS):
    raise ValueError("The main word list contains duplicate words")


def init_database():
    with sqlite3.connect(DATABASE_FILE, timeout=10) as connection:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS mistakes (
                chat_id INTEGER NOT NULL,
                word_index INTEGER NOT NULL,
                wrong_count INTEGER NOT NULL DEFAULT 1,
                last_wrong TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (chat_id, word_index)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS unknown_words (
                chat_id INTEGER NOT NULL,
                word TEXT NOT NULL,
                translation TEXT NOT NULL,
                collection TEXT NOT NULL,
                added_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (chat_id, word, collection)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS mistakes_by_word (
                chat_id INTEGER NOT NULL,
                word TEXT NOT NULL,
                wrong_count INTEGER NOT NULL DEFAULT 1,
                last_wrong TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (chat_id, word)
            )
            """
        )
        connection.execute(
            "CREATE TABLE IF NOT EXISTS bot_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        migrated = connection.execute(
            "SELECT value FROM bot_meta WHERE key = 'mistakes_by_word_migrated'"
        ).fetchone()
        if migrated is None:
            rows = connection.execute(
                "SELECT chat_id, word_index, wrong_count, last_wrong FROM mistakes"
            ).fetchall()
            connection.executemany(
                """
                INSERT OR IGNORE INTO mistakes_by_word
                    (chat_id, word, wrong_count, last_wrong)
                VALUES (?, ?, ?, ?)
                """,
                [
                    (chat_id, WORDS[index]["word"], count, last_wrong)
                    for chat_id, index, count, last_wrong in rows
                    if 0 <= index < len(WORDS)
                ],
            )
            connection.execute(
                "INSERT INTO bot_meta (key, value) VALUES ('mistakes_by_word_migrated', '1')"
            )


def record_mistake(chat_id, word_index):
    with sqlite3.connect(DATABASE_FILE, timeout=10) as connection:
        connection.execute(
            """
            INSERT INTO mistakes_by_word (chat_id, word)
            VALUES (?, ?)
            ON CONFLICT (chat_id, word) DO UPDATE SET
                wrong_count = wrong_count + 1,
                last_wrong = CURRENT_TIMESTAMP
            """,
            (chat_id, WORDS[word_index]["word"]),
        )


def mistake_indexes(chat_id):
    with sqlite3.connect(DATABASE_FILE, timeout=10) as connection:
        rows = connection.execute(
            "SELECT word FROM mistakes_by_word WHERE chat_id = ? ORDER BY last_wrong DESC",
            (chat_id,),
        ).fetchall()
    return [WORD_INDEX_BY_WORD[word] for (word,) in rows if word in WORD_INDEX_BY_WORD]


def remove_mistake(chat_id, word_index):
    with sqlite3.connect(DATABASE_FILE, timeout=10) as connection:
        connection.execute(
            "DELETE FROM mistakes_by_word WHERE chat_id = ? AND word = ?",
            (chat_id, WORDS[word_index]["word"]),
        )


def add_unknown(chat_id, item, collection):
    with sqlite3.connect(DATABASE_FILE, timeout=10) as connection:
        connection.execute(
            """
            INSERT INTO unknown_words (chat_id, word, translation, collection)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (chat_id, word, collection) DO UPDATE SET
                translation = excluded.translation
            """,
            (chat_id, item["word"], item["translation"], collection),
        )


def unknown_items(chat_id):
    with sqlite3.connect(DATABASE_FILE, timeout=10) as connection:
        rows = connection.execute(
            """
            SELECT word, translation, collection
            FROM unknown_words
            WHERE chat_id = ?
            ORDER BY added_at
            """,
            (chat_id,),
        ).fetchall()
    return [
        {"number": index, "word": word, "translation": translation, "collection": collection}
        for index, (word, translation, collection) in enumerate(rows, 1)
    ]


def remove_unknown(chat_id, item):
    with sqlite3.connect(DATABASE_FILE, timeout=10) as connection:
        connection.execute(
            "DELETE FROM unknown_words WHERE chat_id = ? AND word = ? AND collection = ?",
            (chat_id, item["word"], item["collection"]),
        )


init_database()


def load_state():
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


STATE = load_state()


def save_state():
    with STATE_LOCK:
        temporary = STATE_FILE.with_suffix(".tmp")
        temporary.write_text(json.dumps(STATE, ensure_ascii=False), encoding="utf-8")
        temporary.replace(STATE_FILE)


def sync_saved_word_queues():
    changed = False
    for state in STATE.values():
        if not isinstance(state, dict) or state.get("mode") not in WORD_MODES:
            continue
        queue = state.get("queue")
        if not isinstance(queue, list):
            continue
        saved_words = state.get("queue_words")
        if saved_words is None:
            if all(isinstance(index, int) and 0 <= index < len(WORDS) for index in queue):
                state["queue_words"] = [WORDS[index]["word"] for index in queue]
                changed = True
            continue
        if not isinstance(saved_words, list):
            continue

        position = state.get("position", 0)
        if not isinstance(position, int):
            position = 0
        mapped_queue = []
        kept_words = []
        mapped_position = 0
        for original_position, word in enumerate(saved_words):
            index = WORD_INDEX_BY_WORD.get(word)
            if index is None:
                continue
            mapped_queue.append(index)
            kept_words.append(word)
            if original_position < position:
                mapped_position += 1

        if not mapped_queue:
            state["mode"] = "idle"
            changed = True
            continue
        mapped_position = min(mapped_position, len(mapped_queue) - 1)
        if queue != mapped_queue or saved_words != kept_words or position != mapped_position:
            state["queue"] = mapped_queue
            state["queue_words"] = kept_words
            state["position"] = mapped_position
            changed = True
    if changed:
        save_state()


sync_saved_word_queues()


def user_state(chat_id):
    with STATE_LOCK:
        key = str(chat_id)
        if key not in STATE:
            STATE[key] = {"direction": "en_ru", "mode": "idle", "send_voice": True}
        STATE[key].setdefault("direction", "en_ru")
        STATE[key].setdefault("mode", "idle")
        STATE[key].setdefault("send_voice", True)
        return STATE[key]


def chat_lock(chat_id):
    with STATE_LOCK:
        return CHAT_LOCKS.setdefault(chat_id, threading.Lock())


def menu_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=4)
    buttons = []
    for level in range((len(WORDS) + 499) // 500):
        first = level * 500 + 1
        last = min(first + 499, len(WORDS))
        buttons.append(InlineKeyboardButton(f"{level + 1}  {first}-{last}", callback_data=f"level:{level}"))
    keyboard.row(*buttons[:4])
    keyboard.row(*buttons[4:8])
    keyboard.row(*buttons[8:])
    keyboard.row(InlineKeyboardButton("Тест", callback_data="test"))
    keyboard.row(
        InlineKeyboardButton("Мои ошибки", callback_data="mistakes"),
        InlineKeyboardButton("Не знаю", callback_data="unknowns"),
        InlineKeyboardButton("Свой уровень", callback_data="custom"),
    )
    keyboard.row(
        InlineKeyboardButton("Фразовые глаголы", callback_data="phrasal"),
        InlineKeyboardButton("Экспорт TXT", callback_data="export"),
        InlineKeyboardButton("Настройки", callback_data="settings"),
    )
    return keyboard


def export_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("Не знаю", callback_data="export:unknowns"),
        InlineKeyboardButton("Мои ошибки", callback_data="export:mistakes"),
        InlineKeyboardButton("Всё сложное", callback_data="export:combined"),
        InlineKeyboardButton("Диапазон", callback_data="export:range"),
    )
    keyboard.add(InlineKeyboardButton("Меню", callback_data="menu"))
    return keyboard


def settings_keyboard(send_voice):
    label = "Голосовые включены" if send_voice else "Голосовые выключены"
    value = "off" if send_voice else "on"
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton(label, callback_data=f"voice_send:{value}"))
    keyboard.add(InlineKeyboardButton("Меню", callback_data="menu"))
    return keyboard


def answer_keyboard(options, word_index, question_id, mode):
    keyboard = InlineKeyboardMarkup(row_width=1)
    for position, option in enumerate(options):
        keyboard.add(InlineKeyboardButton(option, callback_data=f"answer:{question_id}:{word_index}:{position}"))
    if mode == "unknowns":
        keyboard.add(
            InlineKeyboardButton(
                "Удалить из карточки",
                callback_data=f"unknown:remove:{question_id}:{word_index}",
            )
        )
    else:
        keyboard.add(
            InlineKeyboardButton(
                "Не знаю",
                callback_data=f"unknown:add:{question_id}:{word_index}",
            )
        )
    return keyboard


def direction_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton("Английский → Русский", callback_data="direction:en_ru"))
    keyboard.add(InlineKeyboardButton("Русский → Английский", callback_data="direction:ru_en"))
    keyboard.add(InlineKeyboardButton("Меню", callback_data="menu"))
    return keyboard


def items_for_state(state):
    if state.get("mode") == "phrasal":
        return PHRASAL_VERBS
    if state.get("mode") == "unknowns":
        return state.get("unknown_items", [])
    return WORDS


def is_current_question(state, question_id, word_index):
    queue = state.get("queue", [])
    position = state.get("position", -1)
    return (
        state.get("mode") in {"level", "test", "custom", "mistakes", "phrasal", "unknowns"}
        and isinstance(position, int)
        and 0 <= position < len(queue)
        and queue[position] == word_index
        and state.get("question_id") == question_id
    )


def nearby_options(items, word_index, correct, direction):
    start = max(0, word_index - 40)
    finish = min(len(items), word_index + 41)
    key = "translation" if direction == "en_ru" else "word"
    pool = [item[key] for item in items[start:finish] if item[key] != correct]
    unique = list(dict.fromkeys(pool))

    if len(unique) < 2:
        unique = list(dict.fromkeys(
            item[key] for item in (*items, *WORDS, *PHRASAL_VERBS)
            if item[key] != correct
        ))

    return random.sample(unique, 2)


def set_question(chat_id):
    state = user_state(chat_id)
    items = items_for_state(state)
    index = state["queue"][state["position"]]
    item = items[index]
    answer_key = "translation" if state["direction"] == "en_ru" else "word"
    correct = item[answer_key]
    options = nearby_options(items, index, correct, state["direction"])
    options.append(correct)
    random.shuffle(options)
    state["options"] = options
    state["question_id"] = state.get("question_id", 0) + 1
    save_state()
    return item, options


def question_text(item, mode, position, direction, total=None):
    question = item["word"] if direction == "en_ru" else item["translation"]
    if mode == "test":
        return f"{position + 1} из 50\n{question}"
    if mode == "phrasal":
        return f"{position + 1} из 100\n{question}"
    if mode == "unknowns":
        return f"{position + 1} из {total}\n{question}"
    return f"{item['number']}\n{question}"


def send_question(chat_id, message_id=None):
    state = user_state(chat_id)
    item, options = set_question(chat_id)
    text = question_text(item, state["mode"], state["position"], state["direction"], len(state["queue"]))
    keyboard = answer_keyboard(options, item["number"] - 1, state["question_id"], state["mode"])
    if message_id is None:
        bot.send_message(chat_id, text, reply_markup=keyboard)
    else:
        try:
            bot.edit_message_text(text, chat_id, message_id, reply_markup=keyboard)
        except Exception:
            bot.send_message(chat_id, text, reply_markup=keyboard)

    schedule_voice(
        chat_id,
        item["word"],
        "elevenlabs",
        state["question_id"],
        state["send_voice"],
        notify=state["send_voice"],
    )


def start_level(chat_id, level):
    state = user_state(chat_id)
    queue = list(range(level * 500, min((level + 1) * 500, len(WORDS))))
    state.update(
        {
            "mode": "level",
            "level": level + 1,
            "queue": queue,
            "queue_words": [WORDS[index]["word"] for index in queue],
            "position": 0,
            "errors": 0,
            "unknown": 0,
            "correct": 0,
            "voice_messages": [],
        }
    )
    save_state()


def start_test(chat_id):
    state = user_state(chat_id)
    queue = [
        random.randrange(part * len(WORDS) // 50, (part + 1) * len(WORDS) // 50)
        for part in range(50)
    ]
    state.update(
        {
            "mode": "test",
            "queue": queue,
            "queue_words": [WORDS[index]["word"] for index in queue],
            "position": 0,
            "errors": 0,
            "unknown": 0,
            "correct": 0,
            "voice_messages": [],
        }
    )
    save_state()


def start_custom(chat_id, first, last):
    state = user_state(chat_id)
    queue = list(range(first, last + 1))
    state.update(
        {
            "mode": "custom",
            "custom_first": first + 1,
            "custom_last": last + 1,
            "queue": queue,
            "queue_words": [WORDS[index]["word"] for index in queue],
            "position": 0,
            "errors": 0,
            "unknown": 0,
            "correct": 0,
            "voice_messages": [],
        }
    )
    save_state()


def start_mistakes(chat_id):
    state = user_state(chat_id)
    queue = mistake_indexes(chat_id)
    if not queue:
        return False
    random.shuffle(queue)
    round_size = len(queue)
    queue = queue + queue
    state.update(
        {
            "mode": "mistakes",
            "queue": queue,
            "queue_words": [WORDS[index]["word"] for index in queue],
            "mistake_round_size": round_size,
            "position": 0,
            "errors": 0,
            "unknown": 0,
            "correct": 0,
            "voice_messages": [],
        }
    )
    save_state()
    return True


def start_phrasal(chat_id):
    state = user_state(chat_id)
    state.update(
        {
            "mode": "phrasal",
            "queue": list(range(len(PHRASAL_VERBS))),
            "position": 0,
            "errors": 0,
            "unknown": 0,
            "correct": 0,
            "voice_messages": [],
        }
    )
    state.pop("queue_words", None)
    save_state()


def start_unknowns(chat_id):
    items = unknown_items(chat_id)
    if not items:
        return False
    state = user_state(chat_id)
    state.update(
        {
            "mode": "unknowns",
            "unknown_items": items,
            "queue": list(range(len(items))),
            "position": 0,
            "errors": 0,
            "correct": 0,
            "voice_messages": [],
        }
    )
    state.pop("queue_words", None)
    save_state()
    return True


def choose_direction(chat_id, mode, level=None):
    state = user_state(chat_id)
    state.pop("awaiting_range", None)
    state.pop("awaiting_export_range", None)
    state["pending_mode"] = mode
    state["pending_level"] = level
    save_state()


def finish(chat_id, message_id):
    state = user_state(chat_id)
    if state["mode"] == "level":
        text = f"Уровень {state['level']}\nОшибки {state['errors']}\nНе знаю {state['unknown']}"
    elif state["mode"] == "custom":
        text = (
            f"Уровень {state['custom_first']}-{state['custom_last']}\n"
            f"Ошибки {state['errors']}\nНе знаю {state['unknown']}"
        )
    elif state["mode"] == "test":
        known = round(state["correct"] / 50 * len(WORDS))
        text = (
            f"Тест\nВерно {state['correct']} из 50\nОшибки {state['errors']}\n"
            f"Не знаю {state['unknown']}\nПримерно знаешь {known} слов"
        )
    elif state["mode"] == "phrasal":
        text = (
            f"Фразовые глаголы\nВерно {state['correct']} из {len(PHRASAL_VERBS)}\n"
            f"Ошибки {state['errors']}\nНе знаю {state['unknown']}"
        )
    elif state["mode"] == "unknowns":
        text = f"Карточка пройдена\nОсталось {len(unknown_items(chat_id))}"
    else:
        text = f"Ошибки пройдены\nОсталось {len(mistake_indexes(chat_id))}"

    state["mode"] = "idle"
    save_state()
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton("Меню", callback_data="menu"))
    try:
        bot.edit_message_text(text, chat_id, message_id, reply_markup=keyboard)
    except Exception:
        bot.send_message(chat_id, text, reply_markup=keyboard)


def advance_question(chat_id, message_id):
    state = user_state(chat_id)
    state["position"] += 1
    save_state()
    if state["position"] == len(state["queue"]):
        finish(chat_id, message_id)
    else:
        send_question(chat_id, message_id)


def audio_path(word, mode):
    digest = hashlib.sha1(word.encode("utf-8")).hexdigest()
    return AUDIO_DIR / mode / f"{digest}.ogg"


def delete_voice_messages(chat_id, state):
    for message_id in state.get("voice_messages", []):
        try:
            bot.delete_message(chat_id, message_id)
        except Exception:
            pass
    state["voice_messages"] = []


def delete_feedback_message(chat_id, state):
    message_id = state.pop("feedback_message", None)
    if message_id is None:
        return
    try:
        bot.delete_message(chat_id, message_id)
    except Exception:
        pass


def ensure_voice(word, mode):
    target = audio_path(word, mode)
    target.parent.mkdir(parents=True, exist_ok=True)
    with VOICE_LOCK:
        if target.exists() and target.stat().st_size > 100:
            return target
        target.unlink(missing_ok=True)
        ready = VOICE_JOBS.get(target)
        if ready is None:
            ready = threading.Event()
            VOICE_JOBS[target] = ready
            create = True
        else:
            create = False

    if create:
        try:
            if mode == "online":
                make_online_voice(word, target)
            else:
                make_offline_voice(word, target)
        finally:
            with VOICE_LOCK:
                VOICE_JOBS.pop(target, None)
                ready.set()
    else:
        ready.wait()

    if not target.exists():
        raise RuntimeError("voice was not created")
    return target


def make_offline_voice(word, target):
    temporary_id = f"{os.getpid()}.{threading.get_ident()}"
    wav = target.with_name(f".{target.stem}.{temporary_id}.wav")
    temporary = target.with_name(f".{target.stem}.{temporary_id}.ogg")
    try:
        subprocess.run(["espeak-ng", "-v", "en-us", "-s", "165", "-w", str(wav), word], check=True)
        subprocess.run(
            ["ffmpeg", "-y", "-loglevel", "error", "-i", str(wav), "-filter:a", "atempo=1.2", "-c:a", "libopus", "-b:a", "32k", str(temporary)],
            check=True,
        )
        temporary.replace(target)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    finally:
        wav.unlink(missing_ok=True)


def make_online_voice(word, target):
    temporary_id = f"{os.getpid()}.{threading.get_ident()}"
    mp3 = target.with_name(f".{target.stem}.{temporary_id}.mp3")
    temporary = target.with_name(f".{target.stem}.{temporary_id}.ogg")
    try:
        wait_for_online_slot()
        for domain in ("us", "com"):
            try:
                gTTS(text=word, lang="en", tld=domain, timeout=5).save(str(mp3))
                break
            except Exception:
                mp3.unlink(missing_ok=True)
                if domain == "com":
                    raise
        subprocess.run(
            ["ffmpeg", "-y", "-loglevel", "error", "-i", str(mp3), "-filter:a", "atempo=1.2", "-c:a", "libopus", "-b:a", "32k", str(temporary)],
            check=True,
        )
        temporary.replace(target)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    finally:
        mp3.unlink(missing_ok=True)


def wait_for_online_slot():
    lock_file = AUDIO_DIR / "online" / ".request.lock"
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    with lock_file.open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        lock.seek(0)
        try:
            last_request = float(lock.read() or 0)
        except ValueError:
            last_request = 0
        delay = ONLINE_REQUEST_DELAY - (time.time() - last_request)
        if delay > 0:
            time.sleep(delay)
        lock.seek(0)
        lock.truncate()
        lock.write(str(time.time()))
        lock.flush()
        fcntl.flock(lock, fcntl.LOCK_UN)


def schedule_voice(chat_id, word, mode, question_id, send_to_telegram, notify=False):
    key = (chat_id, question_id)
    with VOICE_LOCK:
        if key in VOICE_SENDS:
            return
        VOICE_SENDS.add(key)

    def run():
        try:
            send_voice(chat_id, word, mode, question_id, send_to_telegram, notify)
        finally:
            with VOICE_LOCK:
                VOICE_SENDS.discard(key)

    threading.Thread(target=run, daemon=True).start()


def play_on_host(target):
    global HOST_PLAYER
    with HOST_PLAYER_LOCK:
        if HOST_PLAYER is not None and HOST_PLAYER.poll() is None:
            HOST_PLAYER.terminate()
        environment = os.environ.copy()
        environment.setdefault("XDG_RUNTIME_DIR", "/run/user/1000")
        environment.setdefault("DBUS_SESSION_BUS_ADDRESS", "unix:path=/run/user/1000/bus")
        HOST_PLAYER = subprocess.Popen(
            ["ffplay", "-nodisp", "-autoexit", "-loglevel", "quiet", str(target)],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=environment,
        )


def send_voice(chat_id, word, mode, question_id, send_to_telegram, notify=False):
    try:
        state = user_state(chat_id)
        if state.get("mode") == "idle" or state.get("question_id") != question_id:
            return
        target = audio_path(word, mode)
        if not target.exists() or target.stat().st_size <= 100:
            target = audio_path(word, "online")
        if not target.exists() or target.stat().st_size <= 100:
            try:
                target = ensure_voice(word, "online")
            except Exception:
                target = audio_path(word, "offline")
        if not target.exists() or target.stat().st_size <= 100:
            target = ensure_voice(word, "offline")
        state = user_state(chat_id)
        if state.get("mode") == "idle" or state.get("question_id") != question_id:
            return
        try:
            play_on_host(target)
        except Exception:
            pass
        if not send_to_telegram:
            return
        message = None
        for attempt in range(2):
            try:
                with target.open("rb") as audio:
                    message = bot.send_voice(chat_id, audio)
                break
            except Exception:
                if attempt == 0:
                    time.sleep(0.5)
        if message is None:
            raise RuntimeError("voice was not sent")
        with chat_lock(chat_id):
            state = user_state(chat_id)
            if state.get("mode") == "idle" or state.get("question_id") != question_id:
                bot.delete_message(chat_id, message.message_id)
                return
            state.setdefault("voice_messages", []).append(message.message_id)
            save_state()
    except Exception:
        state = user_state(chat_id)
        if notify and state.get("mode") != "idle" and state.get("question_id") == question_id:
            bot.send_message(chat_id, "Голос недоступен")


@bot.message_handler(commands=["start"])
def start(message):
    with chat_lock(message.chat.id):
        state = user_state(message.chat.id)
        delete_voice_messages(message.chat.id, state)
        delete_feedback_message(message.chat.id, state)
        state["mode"] = "idle"
        for key in ("pending_mode", "pending_level", "pending_range", "awaiting_range", "awaiting_export_range", "unknown_items"):
            state.pop(key, None)
        save_state()
        bot.send_message(message.chat.id, "Уровни", reply_markup=menu_keyboard())


@bot.message_handler(commands=["resume"])
def resume(message):
    with chat_lock(message.chat.id):
        state = user_state(message.chat.id)
        queue = state.get("queue", [])
        position = state.get("position", -1)
        items = items_for_state(state)
        if (
            state.get("mode") not in {"level", "test", "custom", "mistakes", "phrasal", "unknowns"}
            or not isinstance(position, int)
            or not 0 <= position < len(queue)
            or not 0 <= queue[position] < len(items)
        ):
            bot.send_message(message.chat.id, "Нет незавершённого занятия", reply_markup=menu_keyboard())
            return
        delete_voice_messages(message.chat.id, state)
        delete_feedback_message(message.chat.id, state)
        send_question(message.chat.id)


def parse_custom_range(text):
    match = re.fullmatch(r"\s*(\d{1,4})\s*[-–— ]\s*(\d{1,4})\s*", text)
    if not match:
        return None
    first, last = map(int, match.groups())
    if not 1 <= first <= last <= len(WORDS):
        return None
    return first - 1, last - 1


def send_txt(chat_id, items, filename):
    content = "\n".join(f"{item['number']} {item['word']}" for item in items)
    document = io.BytesIO(content.encode("utf-8"))
    document.name = filename
    bot.send_document(chat_id, document, caption=f"Слов: {len(items)}")


def unknown_word_items(chat_id):
    by_word = {item["word"]: item for item in WORDS}
    return [
        by_word[item["word"]]
        for item in unknown_items(chat_id)
        if item["collection"] == "words" and item["word"] in by_word
    ]


def difficult_items(chat_id):
    items = {index: WORDS[index] for index in mistake_indexes(chat_id)}
    by_word = {item["word"]: index for index, item in enumerate(WORDS)}
    for item in unknown_items(chat_id):
        index = by_word.get(item["word"])
        if item["collection"] == "words" and index is not None:
            items[index] = WORDS[index]
    return [items[index] for index in sorted(items)]


@bot.message_handler(content_types=["text"], func=lambda message: user_state(message.chat.id).get("awaiting_range"))
def custom_range(message):
    with chat_lock(message.chat.id):
        selected_range = parse_custom_range(message.text)
        if selected_range is None:
            bot.send_message(message.chat.id, f"Нужно указать от 1 до {len(WORDS)}, например 120-350")
            return

        state = user_state(message.chat.id)
        state.pop("awaiting_range", None)
        state["pending_mode"] = "custom"
        state["pending_range"] = selected_range
        save_state()
        bot.send_message(message.chat.id, "Как будем проходить?", reply_markup=direction_keyboard())


@bot.message_handler(content_types=["text"], func=lambda message: user_state(message.chat.id).get("awaiting_export_range"))
def export_range(message):
    with chat_lock(message.chat.id):
        selected_range = parse_custom_range(message.text)
        if selected_range is None:
            bot.send_message(message.chat.id, f"Нужно указать от 1 до {len(WORDS)}, например 120-350")
            return

        first, last = selected_range
        state = user_state(message.chat.id)
        state.pop("awaiting_export_range", None)
        save_state()
        send_txt(message.chat.id, WORDS[first:last + 1], f"words_{first + 1}_{last + 1}.txt")
        bot.send_message(message.chat.id, "Готово", reply_markup=menu_keyboard())


@bot.callback_query_handler(func=lambda call: True)
def callbacks(call):
    with chat_lock(call.message.chat.id):
        process_callback(call)


def process_callback(call):
    chat_id = call.message.chat.id
    state = user_state(chat_id)
    data = call.data

    if data == "menu":
        bot.answer_callback_query(call.id)
        delete_voice_messages(chat_id, state)
        delete_feedback_message(chat_id, state)
        state["mode"] = "idle"
        state.pop("pending_mode", None)
        state.pop("pending_level", None)
        state.pop("pending_range", None)
        state.pop("awaiting_range", None)
        state.pop("awaiting_export_range", None)
        state.pop("unknown_items", None)
        save_state()
        try:
            bot.edit_message_text("Уровни", chat_id, call.message.message_id, reply_markup=menu_keyboard())
        except Exception:
            bot.send_message(chat_id, "Уровни", reply_markup=menu_keyboard())
        return

    if data == "export":
        state.pop("awaiting_range", None)
        state.pop("awaiting_export_range", None)
        bot.answer_callback_query(call.id)
        try:
            bot.edit_message_text("Что выгрузить?", chat_id, call.message.message_id, reply_markup=export_keyboard())
        except Exception:
            bot.send_message(chat_id, "Что выгрузить?", reply_markup=export_keyboard())
        return

    if data.startswith("export:"):
        export_type = data.split(":", 1)[1]
        bot.answer_callback_query(call.id)
        if export_type == "range":
            state.pop("awaiting_range", None)
            state["awaiting_export_range"] = True
            save_state()
            keyboard = InlineKeyboardMarkup(row_width=1)
            keyboard.add(InlineKeyboardButton("Меню", callback_data="menu"))
            try:
                bot.edit_message_text(
                    "Введи диапазон, например 120-350",
                    chat_id,
                    call.message.message_id,
                    reply_markup=keyboard,
                )
            except Exception:
                bot.send_message(chat_id, "Введи диапазон, например 120-350", reply_markup=keyboard)
            return

        if export_type == "unknowns":
            items = unknown_word_items(chat_id)
            filename = "unknown_words.txt"
        elif export_type == "mistakes":
            items = [WORDS[index] for index in sorted(mistake_indexes(chat_id))]
            filename = "mistakes.txt"
        else:
            items = difficult_items(chat_id)
            filename = "difficult_words.txt"

        if not items:
            bot.send_message(chat_id, "Список пока пуст", reply_markup=export_keyboard())
            return
        send_txt(chat_id, items, filename)
        return

    if data == "settings":
        bot.answer_callback_query(call.id)
        try:
            bot.edit_message_text(
                "Настройки",
                chat_id,
                call.message.message_id,
                reply_markup=settings_keyboard(state["send_voice"]),
            )
        except Exception:
            bot.send_message(chat_id, "Настройки", reply_markup=settings_keyboard(state["send_voice"]))
        return

    if data.startswith("voice_send:"):
        state["send_voice"] = data.split(":", 1)[1] == "on"
        save_state()
        bot.answer_callback_query(call.id)
        bot.edit_message_text(
            "Настройки",
            chat_id,
            call.message.message_id,
            reply_markup=settings_keyboard(state["send_voice"]),
        )
        return

    if data.startswith("level:"):
        state.pop("awaiting_range", None)
        level = int(data.split(":", 1)[1])
        if not 0 <= level < (len(WORDS) + 499) // 500:
            bot.answer_callback_query(call.id, "Уровень больше недоступен")
            try:
                bot.edit_message_text("Уровни", chat_id, call.message.message_id, reply_markup=menu_keyboard())
            except Exception:
                bot.send_message(chat_id, "Уровни", reply_markup=menu_keyboard())
            return
        choose_direction(chat_id, "level", level)
        bot.answer_callback_query(call.id)
        try:
            bot.edit_message_text("Как будем проходить?", chat_id, call.message.message_id, reply_markup=direction_keyboard())
        except Exception:
            bot.send_message(chat_id, "Как будем проходить?", reply_markup=direction_keyboard())
        return

    if data == "test":
        state.pop("awaiting_range", None)
        choose_direction(chat_id, "test")
        bot.answer_callback_query(call.id)
        try:
            bot.edit_message_text("Как будем проходить?", chat_id, call.message.message_id, reply_markup=direction_keyboard())
        except Exception:
            bot.send_message(chat_id, "Как будем проходить?", reply_markup=direction_keyboard())
        return

    if data == "phrasal":
        state.pop("awaiting_range", None)
        choose_direction(chat_id, "phrasal")
        bot.answer_callback_query(call.id)
        try:
            bot.edit_message_text("Как будем проходить?", chat_id, call.message.message_id, reply_markup=direction_keyboard())
        except Exception:
            bot.send_message(chat_id, "Как будем проходить?", reply_markup=direction_keyboard())
        return

    if data == "unknowns":
        state.pop("awaiting_range", None)
        bot.answer_callback_query(call.id)
        if not unknown_items(chat_id):
            try:
                bot.edit_message_text("Карточка пока пустая", chat_id, call.message.message_id, reply_markup=menu_keyboard())
            except Exception:
                bot.send_message(chat_id, "Карточка пока пустая", reply_markup=menu_keyboard())
            return
        choose_direction(chat_id, "unknowns")
        try:
            bot.edit_message_text("Как будем проходить?", chat_id, call.message.message_id, reply_markup=direction_keyboard())
        except Exception:
            bot.send_message(chat_id, "Как будем проходить?", reply_markup=direction_keyboard())
        return

    if data == "custom":
        state.pop("pending_mode", None)
        state.pop("pending_level", None)
        state.pop("pending_range", None)
        state.pop("awaiting_export_range", None)
        state["awaiting_range"] = True
        state["mode"] = "idle"
        save_state()
        bot.answer_callback_query(call.id)
        keyboard = InlineKeyboardMarkup(row_width=1)
        keyboard.add(InlineKeyboardButton("Меню", callback_data="menu"))
        try:
            bot.edit_message_text(
                "Введи диапазон, например 120-350",
                chat_id,
                call.message.message_id,
                reply_markup=keyboard,
            )
        except Exception:
            bot.send_message(chat_id, "Введи диапазон, например 120-350", reply_markup=keyboard)
        return

    if data == "mistakes":
        state.pop("awaiting_range", None)
        bot.answer_callback_query(call.id)
        if not mistake_indexes(chat_id):
            try:
                bot.edit_message_text("Ошибок пока нет", chat_id, call.message.message_id, reply_markup=menu_keyboard())
            except Exception:
                bot.send_message(chat_id, "Ошибок пока нет", reply_markup=menu_keyboard())
            return
        choose_direction(chat_id, "mistakes")
        try:
            bot.edit_message_text("Как будем проходить?", chat_id, call.message.message_id, reply_markup=direction_keyboard())
        except Exception:
            bot.send_message(chat_id, "Как будем проходить?", reply_markup=direction_keyboard())
        return

    if data.startswith("direction:"):
        mode = state.get("pending_mode")
        if mode not in {"level", "test", "custom", "mistakes", "phrasal", "unknowns"}:
            bot.answer_callback_query(call.id)
            return

        direction = data.split(":", 1)[1]
        if direction not in {"en_ru", "ru_en"}:
            bot.answer_callback_query(call.id)
            return
        state["direction"] = direction
        level = state.pop("pending_level", None)
        custom_range = state.pop("pending_range", None)
        state.pop("pending_mode", None)
        delete_voice_messages(chat_id, state)
        delete_feedback_message(chat_id, state)
        if mode == "level" and level is None:
            state["mode"] = "idle"
            save_state()
            bot.answer_callback_query(call.id, "Уровень больше недоступен")
            bot.send_message(chat_id, "Уровни", reply_markup=menu_keyboard())
            return
        if mode == "level":
            start_level(chat_id, level)
        elif mode == "test":
            start_test(chat_id)
        elif mode == "custom":
            if custom_range is None:
                state["mode"] = "idle"
                save_state()
                bot.answer_callback_query(call.id, "Диапазон больше недоступен")
                bot.send_message(chat_id, "Уровни", reply_markup=menu_keyboard())
                return
            start_custom(chat_id, *custom_range)
        elif mode == "phrasal":
            start_phrasal(chat_id)
        elif mode == "unknowns":
            if not start_unknowns(chat_id):
                state["mode"] = "idle"
                save_state()
                bot.answer_callback_query(call.id, "Карточка пока пустая")
                bot.send_message(chat_id, "Уровни", reply_markup=menu_keyboard())
                return
        elif not start_mistakes(chat_id):
            bot.answer_callback_query(call.id)
            bot.edit_message_text("Ошибок пока нет", chat_id, call.message.message_id, reply_markup=menu_keyboard())
            return
        bot.answer_callback_query(call.id)
        send_question(chat_id, call.message.message_id)
        return

    if data.startswith("unknown:"):
        _, action, question_id, word_index = data.split(":")
        question_id = int(question_id)
        word_index = int(word_index)
        if not is_current_question(state, question_id, word_index):
            bot.answer_callback_query(call.id, "Вопрос изменился. Напишите /resume")
            return

        item = items_for_state(state)[word_index]
        delete_voice_messages(chat_id, state)
        delete_feedback_message(chat_id, state)
        if action == "add" and state["mode"] != "unknowns":
            collection = "phrasal" if state["mode"] == "phrasal" else "words"
            add_unknown(chat_id, item, collection)
            state["unknown"] = state.get("unknown", 0) + 1
            bot.answer_callback_query(call.id, "Добавлено в карточку")
        elif action == "remove" and state["mode"] == "unknowns":
            remove_unknown(chat_id, item)
            bot.answer_callback_query(call.id, "Удалено из карточки")
        else:
            bot.answer_callback_query(call.id)
            return
        advance_question(chat_id, call.message.message_id)
        return

    if not data.startswith("answer:"):
        bot.answer_callback_query(call.id)
        return

    parts = data.split(":")
    if len(parts) == 4:
        _, question_id, word_index, position = parts
        question_id = int(question_id)
    else:
        _, word_index, position = parts
        question_id = state.get("question_id")
    word_index = int(word_index)
    position = int(position)
    if not is_current_question(state, question_id, word_index):
        bot.answer_callback_query(call.id, "Вопрос изменился. Напишите /resume")
        return
    if not 0 <= position < len(state.get("options", [])):
        bot.answer_callback_query(call.id, "Вопрос изменился. Напишите /resume")
        return

    answer_key = "translation" if state["direction"] == "en_ru" else "word"
    correct = items_for_state(state)[word_index][answer_key]
    delete_voice_messages(chat_id, state)
    delete_feedback_message(chat_id, state)
    if state["options"][position] == correct:
        state["correct"] += 1
        if state["mode"] == "mistakes" and state["position"] >= state.get(
            "mistake_round_size", len(state["queue"])
        ):
            remove_mistake(chat_id, word_index)
        bot.answer_callback_query(call.id, "Верно")
    else:
        state["errors"] += 1
        if state["mode"] not in {"phrasal", "unknowns"}:
            record_mistake(chat_id, word_index)
        bot.answer_callback_query(call.id, "Ошибка")
        feedback = bot.send_message(chat_id, f"❌ Неверно\n{correct}")
        state["feedback_message"] = feedback.message_id

    advance_question(chat_id, call.message.message_id)


if __name__ == "__main__":
    bot.infinity_polling(skip_pending=True)
