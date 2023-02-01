## Создание инфраструктуры вокруг модели машинного обучения, дизайн ML-системы

# Содержание

- [Задача](#task1)
- [Инфраструктура](#task2)
    - [Идея](#task2_1)
    - [Реализация](#task2_2)
    - [Что мы хотели бы добавить](#task2_3)
- [ML-модель](#task4)
    - [Реализация](#task4_1)
    - [На чем обучается](#task4_2)
    - [Метрики](#task4_3)

# Задача <a class="anchor" id="task1"></a>
Необходимо создать ML-модель которая будет работать, переобучаться и парсить данные без вмешательства пользователя. То есть задача фактически звучит как "нужно вывести модель в прод". Основным требованием является самостоятельность модели, она должна переобучаться на новых данных, а также возможность просмотреть историю метрик модели - растут метрики, падают или остаются неизменны, для того чтобы определять "выдохлась" модель или нет.

# Инфраструктура <a class="anchor" id="task2"></a>

## Идея <a class="anchor" id="task2_1"></a>
Хотелось реализовать модель которая будет существовать внутри инфраструктуры максимально приближенной к "продовой", но при этом не усложнять проект. Поэтому решено реализовать всю инфраструктуру в docker-контейнерах на следующем ПО: airflow, mlflow, postgres, superset, jupyter. Такой набор позволит автоматизировать все процессы, отслеживать метрики после каждого переобучения, хранить все данные в БД (чтобы уйти от csv) и визуализировать данные в BI-системе.

## Реализация <a class="anchor" id="task2_2"></a>
Под каждый сервис поднят отдельный сервис, все сервисы связаны томами через docker, superset и airflow подключены к postgres. В jupyter прокинута папка dags которая связана с контейнером airflow, это позволяет добавлять даги в airflow и оперативно поправлять даги после тестирования. Каждый контейнер доступен из другого так как все контейнеры находятся в одной сети. 

Ежедневно запускаются даги которые парсят новые данные, скачивают и кладут их в БД. Отдельно парсятся данные фактических вылетов и будущее расписание. Расписание и фактические вылеты вставляются в отдельные таблицы. При этом таблица с расписанием всегда содержит только 2 дня: текущий и завтрашний. При добавлении данных в таблицу остальные данные удаляются. Таблицы с фактическими вылетами содержит в себе исторические данные начиная с 1 января 2021 года.

Аналогично собираются данные о погоде - данные о фактический погоде и прогноз вставляются в 2 разные таблицы. Точно также прогноз всегда содержит 2 дня - сегодня и завтра. Таблица с фактом погоды содержит данные начиная с 1 января 2021 года.

После того как загружены данные запускается даг с переобучением модели, после обучения модель логирует метрики в mlflow и обновляет таблицу с предсказанием вылетов в БД. После из этой таблицы строится дашборд в superset. 
