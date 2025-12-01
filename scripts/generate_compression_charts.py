#!/usr/bin/env python3
"""
Генерация графиков для отчета лабораторной работы №4
Сравнение алгоритмов сжатия LZ77, GZIP, ZSTD
"""

import matplotlib.pyplot as plt
import numpy as np

# Настройка для русского языка
plt.rcParams['font.family'] = 'DejaVu Sans'
plt.rcParams['font.size'] = 10

# Данные для trade_data (278.38 MB)
algorithms = ['LZ77\n(наша)', 'GZIP', 'ZSTD']
compression_ratios = [2.29, 5.67, 4.85]  # коэффициенты сжатия
compress_times = [187.32, 22.67, 1.99]  # секунды
decompress_times = [203.15, 1.73, 0.71]  # секунды

# График 1: Коэффициенты сжатия
fig1, ax1 = plt.subplots(figsize=(10, 6))
bars = ax1.bar(algorithms, compression_ratios, color=['#e74c3c', '#3498db', '#2ecc71'])
ax1.set_ylabel('Коэффициент сжатия', fontsize=12)
ax1.set_title('Коэффициенты сжатия для trade_data (278.38 MB)', fontsize=14, fontweight='bold')
ax1.grid(axis='y', alpha=0.3)

# Добавляем значения на столбцы
for bar, ratio in zip(bars, compression_ratios):
    height = bar.get_height()
    ax1.text(bar.get_x() + bar.get_width()/2., height,
            f'{ratio:.2f}×',
            ha='center', va='bottom', fontsize=11, fontweight='bold')

plt.tight_layout()
plt.savefig('docs/images/lab4_compression_ratio.png', dpi=300, bbox_inches='tight')
print("✓ График 1 сохранен: docs/images/lab4_compression_ratio.png")
plt.close()

# График 2: Время сжатия
fig2, ax2 = plt.subplots(figsize=(10, 6))
bars = ax2.bar(algorithms, compress_times, color=['#e74c3c', '#3498db', '#2ecc71'])
ax2.set_ylabel('Время (секунды)', fontsize=12)
ax2.set_title('Время сжатия для trade_data (278.38 MB)', fontsize=14, fontweight='bold')
ax2.grid(axis='y', alpha=0.3)
ax2.set_yscale('log')  # Логарифмическая шкала для лучшей визуализации

# Добавляем значения на столбцы
for bar, time in zip(bars, compress_times):
    height = bar.get_height()
    ax2.text(bar.get_x() + bar.get_width()/2., height,
            f'{time:.2f} с',
            ha='center', va='bottom', fontsize=10, fontweight='bold')

plt.tight_layout()
plt.savefig('docs/images/lab4_compress_time.png', dpi=300, bbox_inches='tight')
print("✓ График 2 сохранен: docs/images/lab4_compress_time.png")
plt.close()

# График 3: Время распаковки
fig3, ax3 = plt.subplots(figsize=(10, 6))
bars = ax3.bar(algorithms, decompress_times, color=['#e74c3c', '#3498db', '#2ecc71'])
ax3.set_ylabel('Время (секунды)', fontsize=12)
ax3.set_title('Время распаковки для trade_data (278.38 MB)', fontsize=14, fontweight='bold')
ax3.grid(axis='y', alpha=0.3)
ax3.set_yscale('log')  # Логарифмическая шкала

# Добавляем значения на столбцы
for bar, time in zip(bars, decompress_times):
    height = bar.get_height()
    ax3.text(bar.get_x() + bar.get_width()/2., height,
            f'{time:.2f} с',
            ha='center', va='bottom', fontsize=10, fontweight='bold')

plt.tight_layout()
plt.savefig('docs/images/lab4_decompress_time.png', dpi=300, bbox_inches='tight')
print("✓ График 3 сохранен: docs/images/lab4_decompress_time.png")
plt.close()

print("\nВсе графики успешно созданы!")

