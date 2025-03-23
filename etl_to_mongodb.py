# Загрузка данных из CSV-файла в MongoDB. Код для ETL-процесса
import pandas as pd
from pymongo import MongoClient

# 1. Читаем CSV-файл
csv_file_path = 'Loan_Default_final_metrics_capped.csv'
df = pd.read_csv(csv_file_path)

# 2. Преобразуем DataFrame в список словарей
# MongoDB хранит данные как JSON-документы, поэтому преобразуем строки в словари
data = df.to_dict('records')

# 3. Подключаемся к MongoDB
client = MongoClient('mongodb://localhost:27017/')  # Адрес моего локального сервера
db = client['loan_risk_db']  # Выбираем базу данных
collection = db['loan_metrics']  # Выбираем коллекцию

# 4. Очищаем коллекцию перед загрузкой (опционально)
# Это удалит все старые данные в коллекции, чтобы избежать дубликатов
collection.delete_many({})

# 5. Загружаем данные в MongoDB
collection.insert_many(data)

# 6. Проверяем, сколько записей загружено
print(f"Загружено записей: {collection.count_documents({})}")

# Закрываем соединение 
client.close()

