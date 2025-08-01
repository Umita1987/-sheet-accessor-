# Scheduler Assignment

## Описание
Скрипт подбирает базовые времена старта для набора программ под ограничениями, проверяет окна worst-case и выдаёт расписание. Поддерживается два режима:

- **Локальный dry-run** — берёт input из CSV (`sample_input.csv`), не требует доступа к Google.  
- **Работа с Google Sheets** — читает/пишет прямо в таблицу через сервисный аккаунт.

### Ограничения, которые соблюдаются при подборе
1. Все программы должны завершиться в рамках одних суток (worst-case).  
2. Одновременно не должно работать больше 4 программ (по worst-case перекрытиям).  
3. Программы с одинаковым идентификатором стартуют минимум через 96 минут (чтобы давать запас ≥60 минут с учётом рандомизаций).  
4. Программы с разными идентификаторами имеют базовые старты с разницей минимум в 1 минуту.  
5. Не меняются длительности и рандомизации.  
6. Подбор базовых стартов старается избегать конфликтов по запуску (по worst-case окнам).

## Требования
- Python 3.10+  
- Файл `requirements.txt` с зависимостями:  
  ```
  pandas
  gspread
  google-auth
  ```

## Установка

```bash
# создать виртуальное окружение (рекомендуется)
python -m venv .venv

# активировать
# Windows:
.\.venv\Scripts\Activate
# Unix/macOS:
source .venv/bin/activate

# обновить pip и установить зависимости
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## Формат локального входа (`sample_input.csv`)

Пример минимального `sample_input.csv`:

```csv
NAME,PASSWORD,PID,TIME START,RANDOM START, h,WORKING TIME, h,RANDOM WORKING TIME, h,IDENTIFICATOR
program1,password1,pid123123,,0.3,5,1.5,ac=*-666666
program2,password2,pid123123,,0.3,5,1.5,ac=*-666666
program3,password3,pid123123,,0.3,5,1.5,ac=*-333333
program4,password4,pid123123,,0.3,5,1.5,ac=*-666666
program5,password5,pid123123,,0.3,5,1.5,ac=*-7777
program6,password6,pid123123,,0.3,5,1.5,ac=*-666555
program7,password7,pid123123,,0.3,5,1.5,ac=*-7777
program8,password8,pid123123,,0.3,5,1.5,ac=*-666555
```

- `NAME` должен начинаться с `program` (используется для фильтрации).  
- `RANDOM START, h`, `WORKING TIME, h`, `RANDOM WORKING TIME, h` — параметры в часах.  
- `IDENTIFICATOR` может быть в форме `ac=*-666666` или просто `666666`; скрипт извлечёт идентификатор.  
- `TIME START` можно оставить пустым — будет заполнено.

Файл должен быть в кодировке UTF-8.

## Примеры запуска

### 1. Локально (без Google Sheets):
```bash
python main.py --local sample_input.csv --output result.csv
```
- Использует `sample_input.csv` как вход.  
- Не требует `creds.json` или доступа в интернет.  
- Пишет результат в `result.csv`.

### 2. С Google Sheets:
- Положи файл `creds.json` (ключ сервисного аккаунта Google) рядом со скриптом.  
- Убедись, что сервисный аккаунт имеет доступ к таблице (через "Поделиться").  
- По умолчанию скрипт использует встроенный в код URL/ключ таблицы, либо можно указать явно:

```bash
python main.py --output result.csv
# или
python main.py --sheet <URL или ключ таблицы> --output result.csv
```

- Обновит колонки `TIME START`, `earliest_start`, `latest_end` в Google Sheet и сохранит локальный `result.csv`.

## Выходные файлы

- `result.csv` — итоговое расписание с подобранными базовыми стартами и worst-case окнами. Пример колонок:

```csv
NAME,IDENTIFICATOR,TIME START,earliest_start,latest_end
program1,666666,00:18,00:00,07:06
...
```

## Поведение и fallback

- Если не указан `--local` и не удаётся подключиться к Google Sheets (нет `creds.json` или ошибка доступа), скрипт автоматически попытается загрузить `sample_input.csv` из текущей директории.  
- Всегда сохраняется локальный `result.csv`, даже если запись в Google не удалась.

## Ошибки и отладка

- `ModuleNotFoundError` → установи зависимости: `pip install -r requirements.txt`.  
- Проблемы с доступом к Google Sheets: проверь `creds.json`, расшаренность таблицы по email сервисного аккаунта, и что API Sheets/Drive включены в проекте Google Cloud.  
- Неправильный формат входного CSV: проверь наличие обязательных заголовков (`NAME`, `IDENTIFICATOR`, `RANDOM START, h`, `WORKING TIME, h`, `RANDOM WORKING TIME, h`).

## Пример полного запуска

```bash
# локальный тест
python main.py --local sample_input.csv

# через Google Sheets, результат параллельно в таблицу и в локальный CSV
```

## Дальнейшие улучшения (опционально)

- Генерация визуализации перекрытий (например, гистограмма или временная шкала).  
- Юнит-тесты для ограничений (идентификаторы, перекрытия, 24h и т.п.).
