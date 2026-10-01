import argparse
import time

import bot

COOLDOWN = 120


def cache_word(word):
    try:
        bot.ensure_voice(word)
        return None
    except Exception as error:
        return f"{type(error).__name__}: {error}"


def all_words():
    items = bot.WORDS + bot.PHRASAL_VERBS
    return list(dict.fromkeys(item["word"] for item in items))


def main():
    argparse.ArgumentParser(description="Пополнение кэша онлайн-озвучки").parse_args()
    words = []
    for word in all_words():
        target = bot.audio_path(word)
        if not target.exists() or target.stat().st_size <= 100:
            words.append(word)

    total = len(words)
    ready = 0
    failed = []
    print(f"Нужно создать: {total}", flush=True)
    for index, word in enumerate(words, 1):
        for attempt in range(3):
            error = cache_word(word)
            if error is None:
                ready += 1
                break
            print(f"Ошибка для {word}: {error}", flush=True)
            if attempt < 2:
                print(f"Повтор через {COOLDOWN} секунд", flush=True)
                time.sleep(COOLDOWN)
        else:
            failed.append(word)
            print("Остановка после повторной ошибки; следующий запуск продолжит с недостающих записей", flush=True)
            break
        if index % 25 == 0 or index == total:
            print(f"Обработано: {index}/{total}, создано: {ready}, ошибок: {len(failed)}", flush=True)
    if failed:
        print("Не удалось создать: " + ", ".join(failed), flush=True)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
