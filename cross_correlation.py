import pyhdfs
import csv
import io
import random
import json
from collections import defaultdict
from faker import Faker

fs = pyhdfs.HdfsClient(hosts='localhost:50070', user_name='osboxes')

def read_orders_from_hdfs(path):
    orders = []
    with fs.open(path) as f:
        for line in f:
            line_str = line.decode('utf-8').strip()
            if not line_str or line_str == 'Order ID,Items':  # Пропускаем пустые строки и заголовок
                continue
            try:
                order_id, items_str = line_str.split(',', 1)  # Исправлено на использование запятой в качестве разделителя
                items = items_str.split(';')  # Предполагаем, что элементы разделены точкой с запятой
                order = {
                    'order_id': order_id,
                    'items': items
                }
                orders.append(order)
            except ValueError as e:
                print(f"Ошибка разбора строки '{line_str}': {e}")
    return orders
# Mapper stripes
def map_stripes(order):
    items = order['items']
    stripes = []
    for item in items:
        stripe = defaultdict(int)
        for other_item in items:
            if other_item != item:
                stripe[other_item] += 1
        stripes.append((item, stripe))
    return stripes
# Reducer stripes
def reduce_stripes(stripes_list):
    counts = defaultdict(lambda: defaultdict(int))
    for item, stripe in stripes_list:
        for other_item, count in stripe.items():
            counts[item][other_item] += count
    return counts

def read_products_from_hdfs(path):
    products = []
    with fs.open(path) as f:
        reader = csv.reader(f.read().decode('utf-8').splitlines())
        next(reader)  
        # Пропускаем заголовок
        for row in reader:
            products.append(row[0])  # Предполагаем, что название товара находится в первой колонке
    return products

def map_pairs(order):
    items = order['items']
    pairs = []
    for i in range(len(items)):
        for j in range(i+1, len(items)):
            pairs.append(((items[i], items[j]), 1))
            pairs.append(((items[j], items[i]), 1))
    return pairs

def reduce_pairs(pairs):
    counts = defaultdict(int)
    for pair, count in pairs:
        counts[pair] += count
    return counts

def generate_orders(num_orders: int, products: list[str], max_items: int, hdfs_path: str):
    """
    Генерирует список заказов со случайным набором товаров и записывает их в HDFS в формате CSV.
    
    :param num_orders: Количество заказов.
    :param products: Список доступных товаров.
    :param max_items: Максимальное количество товаров в одном заказе.
    :param hdfs_path: Путь к файлу в HDFS, куда будет записан CSV.
    """
    fake = Faker()
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Запись заголовка
    writer.writerow(['Order ID', 'Items'])

    for _ in range(num_orders):
        num_items = random.randint(1, min(max_items, len(products)))  # Учитываем количество доступных продуктов
        order_items = random.choices(products, k=num_items)  # Разрешаем повторение товаров
        order_id = fake.uuid4()
        writer.writerow([order_id, ';'.join(order_items)])  # Используем ';' для разделения товаров в одном заказе

    # Получение CSV строки из StringIO
    csv_content = output.getvalue()
    output.close()
    
    # Запись CSV строки в HDFS, используя параметр 'data'
    fs.create(hdfs_path, data=csv_content.encode('utf-8'), overwrite=True)
    print(f"successfully")


# Исправленная версия
def write_results_to_hdfs(path, data):
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Product Pair', 'Count'])
    for pair, count in data.items():
        writer.writerow([f"{pair[0]}, {pair[1]}", count])
    csv_content = output.getvalue()
    # Прямая запись в HDFS без использования 'with'
    fs.create(path, data=csv_content.encode('utf-8'), overwrite=True)

def read_cross_correlation_from_hdfs(path: str) -> dict:
    with fs.open(path) as file:
        data = file.read().decode('utf-8')
        reader = csv.reader(io.StringIO(data))
        next(reader)  # Пропускаем заголовок
        return {(row[0]): int(row[1]) for row in reader}

def recommend_products(product: str, cross_correlation: dict, top_n: int = 10) -> list:
    related_products = defaultdict(int)
    for key, count in cross_correlation.items():
        prod1, prod2 = key.split(", ")  # Разбиение ключа на продукты
        if prod1 == product:
            related_products[prod2] += count
        elif prod2 == product:
            related_products[prod1] += count
    recommended = sorted(related_products.items(), key=lambda x: x[1], reverse=True)[:top_n]
    return [prod for prod, _ in recommended]


products = ['Apple', 'Banana', 'Cherry', 'Date', 'Elderberry']
hdfs_path = '/orders.csv'
generate_orders(100, products, 5, hdfs_path)

# Путь к файлу кросс-корреляции в HDFS
results_path = '/cross_correlation_results.csv'

# Чтение заказов из HDFS
orders_path = '/orders.csv'
orders = read_orders_from_hdfs(orders_path)

# Применяем Map
mapped_orders = [map_pairs(order) for order in orders]
# Собираем все пары вместе перед Reduce
pairs_to_reduce = [pair for sublist in mapped_orders for pair in sublist]
# Применяем Reduce и получаем итоговые кросс-корреляции
cross_correlation_results = reduce_pairs(pairs_to_reduce)

# Запись результатов кросс-корреляции в HDFS
write_results_to_hdfs(results_path, cross_correlation_results)
print("Обработка завершена и результаты записаны в HDFS.")

# Чтение кросс-корреляции из HDFS
cross_correlation_data = read_cross_correlation_from_hdfs(results_path)

# Получение рекомендаций
product_to_recommend = "Apple"
recommendations = recommend_products(product_to_recommend, cross_correlation_data)

# Вывод рекомендаций
print(f"Рекомендации для товара {product_to_recommend}: {recommendations}")
