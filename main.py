# Козаченко Софія МФ-41
# Висновки: Запустивши програму декілька разів змінюючи розмір тексту, я помітила,
# що приблизно на розмірності тексту 100000000 MapReduce починає переганяти стандартний метод
# у випадку коли на ноутбуці кількість ядер дорівнює 4 (як у мене на разі). Якщо число менше,
# наприклад 10000000, стандартний метод працює краще.
# Але навіть якщо поставити кількість потоків 2 і залишити розмірність 100000000,
# то MapReduce виграє.
# Тому MapReduce буде в пріоритеті тоді, коли потрібно обробити дійсно великі об'єми
# та є можливість скористатися декількома потоками (чим більше - тим швидше).

import time
import multiprocessing
from functools import reduce
from collections import Counter

# Генеруємо великий текст
def generate_large_text(size):
    return "/"*size

# Стандартна реалізація підрахунку символів
def standard_count(text):
    return Counter( text)

# Функції повертає ("/", n), n- кількість символів в чанку
def map_function(chunk):
    return Counter(chunk)

def reduce_function(counts):
    return reduce(lambda c1, c2: c1+c2, counts)

# MapReduce з багатопоточностю
def map_reduce(text, num_threads=4):
    chunk_size = len(text)//num_threads
    chunks = [text[i:i+ chunk_size] for i in range(0, len(text),chunk_size)]

    with multiprocessing.Pool(num_threads) as pool:
        mapped = pool.map(map_function, chunks)
        reduced = reduce_function(mapped)

    return reduced

# Функція для порівняння збігу результатів та швидкості
def compare_methods(text):
    # Стандартний метод
    start_time = time.time()
    standard_result = standard_count(text)
    standard_duration = time.time()-start_time
    print(f"Стандартна реалізація: {standard_duration:.5f} секунд")

    # MapReduce
    start_time = time.time()
    map_reduce_result = map_reduce(text)
    map_reduce_duration = time.time()-start_time
    print(f"MapReduce реалізація: {map_reduce_duration:.5f} секунд")

    # Порівняння результатів
    if standard_result == map_reduce_result:
        print("Результати однакові.")
    else:
        print("Результати не однакові.")

    # Висновки щодо швидкості
    if map_reduce_duration < standard_duration:
        print("MapReduce працює швидше.")
    else:
        print("Стандартний метод працює шидше.")

# Тестування
if __name__ == "__main__":
    large_text = generate_large_text(100000000)
    compare_methods(large_text)
