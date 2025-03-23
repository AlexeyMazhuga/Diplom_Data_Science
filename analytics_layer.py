{
 "cells": [
  {
   "cell_type": "markdown",
   "id": "a1bc7b86-fd16-4288-be25-25c6663d640c",
   "metadata": {},
   "source": [
    "Файл создан отдельно для дальнейшего переиспользования кода (например, запускать запросы по расписанию)."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "7a859361-21fe-4f0f-962f-bfb989c947df",
   "metadata": {},
   "outputs": [],
   "source": [
    "# analytics_layer.py\n",
    "from pymongo import MongoClient\n",
    "import pandas as pd\n",
    "\n",
    "def calculate_average_raroc_by_region():\n",
    "    # Подключаемся к MongoDB\n",
    "    client = MongoClient('mongodb://localhost:27017/')\n",
    "    db = client['loan_risk_db']\n",
    "    collection = db['loan_metrics']\n",
    "\n",
    "    # Создаём агрегированный запрос\n",
    "    pipeline = [\n",
    "        {\n",
    "            \"$group\": {\n",
    "                \"_id\": \"$Region\",  # Группируем по полю Region\n",
    "                \"average_raroc\": {\"$avg\": \"$RAROC\"}  # Считаем среднее значение RAROC\n",
    "            }\n",
    "        },\n",
    "        {\n",
    "            \"$sort\": {\"average_raroc\": -1}  # Сортируем по убыванию среднего RAROC\n",
    "        }\n",
    "    ]\n",
    "\n",
    "    # Выполняем запрос\n",
    "    result = collection.aggregate(pipeline)\n",
    "\n",
    "    # Преобразуем результат в DataFrame\n",
    "    result_list = list(result)\n",
    "    df_result = pd.DataFrame(result_list)\n",
    "    df_result = df_result.rename(columns={\"_id\": \"Region\", \"average_raroc\": \"Average_RAROC\"})\n",
    "\n",
    "    # Выводим результат\n",
    "    print(\"Средний RAROC по регионам:\")\n",
    "    print(df_result)\n",
    "\n",
    "    # Закрываем соединение\n",
    "    client.close()\n",
    "\n",
    "if __name__ == \"__main__\":\n",
    "    calculate_average_raroc_by_region()"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.11.9"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
