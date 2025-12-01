#!/usr/bin/env python3
"""
Генерация графиков для отчета лабораторной работы №3
Сравнение LSM и PostgreSQL
"""

import matplotlib.pyplot as plt
import numpy as np

# Настройка для русского языка
plt.rcParams['font.family'] = 'DejaVu Sans'
plt.rcParams['font.size'] = 10

# Данные
datasets = ['trade_data', 'tweets']
csv_sizes = [265.49, 3997.58]  # MB
lsm_sizes = [792.45, 6257.66]  # MB
pg_sizes = [1146.17, 8987.94]  # MB

lsm_read_times = [24.72, 110.60]  # секунды
pg_read_times = [6.93, 52.58]  # секунды

lsm_write_times = [162.74, 578.21]  # секунды
pg_write_times = [294.43, 1521.90]  # секунды

# График 1: Размеры после преобразования
fig1, ax1 = plt.subplots(figsize=(10, 6))
x = np.arange(len(datasets))
width = 0.25

bars1 = ax1.bar(x - width, csv_sizes, width, label='CSV', color='#3498db')
bars2 = ax1.bar(x, lsm_sizes, width, label='LSM', color='#e74c3c')
bars3 = ax1.bar(x + width, pg_sizes, width, label='PostgreSQL', color='#2ecc71')

ax1.set_xlabel('Датасет', fontsize=12)
ax1.set_ylabel('Размер (MB)', fontsize=12)
ax1.set_title('Размеры после преобразования', fontsize=14, fontweight='bold')
ax1.set_xticks(x)
ax1.set_xticklabels(datasets)
ax1.legend()
ax1.grid(axis='y', alpha=0.3)

# Добавляем значения на столбцы
for bars in [bars1, bars2, bars3]:
    for bar in bars:
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.1f}',
                ha='center', va='bottom', fontsize=9)

plt.tight_layout()
plt.savefig('docs/images/lab3_sizes.png', dpi=300, bbox_inches='tight')
print("✓ График 1 сохранен: docs/images/lab3_sizes.png")
plt.close()

# График 2: Время полного чтения
fig2, ax2 = plt.subplots(figsize=(10, 6))
x = np.arange(len(datasets))
width = 0.35

bars1 = ax2.bar(x - width/2, lsm_read_times, width, label='LSM', color='#e74c3c')
bars2 = ax2.bar(x + width/2, pg_read_times, width, label='PostgreSQL', color='#2ecc71')

ax2.set_xlabel('Датасет', fontsize=12)
ax2.set_ylabel('Время (секунды)', fontsize=12)
ax2.set_title('Время полного чтения', fontsize=14, fontweight='bold')
ax2.set_xticks(x)
ax2.set_xticklabels(datasets)
ax2.legend()
ax2.grid(axis='y', alpha=0.3)

# Добавляем значения на столбцы
for bars in [bars1, bars2]:
    for bar in bars:
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.2f}',
                ha='center', va='bottom', fontsize=9)

plt.tight_layout()
plt.savefig('docs/images/lab3_read.png', dpi=300, bbox_inches='tight')
print("✓ График 2 сохранен: docs/images/lab3_read.png")
plt.close()

# График 3: Время записи / импорта
fig3, ax3 = plt.subplots(figsize=(10, 6))
x = np.arange(len(datasets))
width = 0.35

bars1 = ax3.bar(x - width/2, lsm_write_times, width, label='LSM', color='#e74c3c')
bars2 = ax3.bar(x + width/2, pg_write_times, width, label='PostgreSQL', color='#2ecc71')

ax3.set_xlabel('Датасет', fontsize=12)
ax3.set_ylabel('Время (секунды)', fontsize=12)
ax3.set_title('Время записи / импорта', fontsize=14, fontweight='bold')
ax3.set_xticks(x)
ax3.set_xticklabels(datasets)
ax3.legend()
ax3.grid(axis='y', alpha=0.3)

# Добавляем значения на столбцы
for bars in [bars1, bars2]:
    for bar in bars:
        height = bar.get_height()
        ax3.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.2f}',
                ha='center', va='bottom', fontsize=9)

plt.tight_layout()
plt.savefig('docs/images/lab3_write.png', dpi=300, bbox_inches='tight')
print("✓ График 3 сохранен: docs/images/lab3_write.png")
plt.close()

print("\nВсе графики успешно созданы!")

