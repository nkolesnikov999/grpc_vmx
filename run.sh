#!/bin/bash

# Скрипт для запуска gNMI мониторинга интерфейса ae1.0

echo "Запуск gNMI мониторинга интерфейса ae1.0..."
echo "Устройство: 192.168.100.111:57400"
echo "Выходной файл: interface_data.csv"
echo "Для остановки нажмите Ctrl+C"
echo ""

# Проверка наличия Go
if ! command -v go &> /dev/null; then
    echo "Ошибка: Go не установлен"
    exit 1
fi

# Установка зависимостей
echo "Установка зависимостей..."
go mod tidy

# Запуск программы
echo "Запуск программы..."
go run main.go
