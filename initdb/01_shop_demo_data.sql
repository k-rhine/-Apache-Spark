-- =====================================================================
--  Демо-данные для вебинара «Оптимизация PySpark»
--  Схема интернет-магазина с ЗАЛОЖЕННЫМИ проблемами для демонстрации:
--
--   countries   200 строк        → крошечная таблица, кандидат на broadcast
--   products    50 000 строк     → маленькая таблица, кандидат на broadcast
--   customers   2 000 000 строк  → большая таблица, join через shuffle (SortMergeJoin)
--   orders      10 000 000 строк → факт-таблица с ПЕРЕКОСОМ по customer_id:
--                 ~60% заказов   — customer_id = 1 (крупный оптовый партнёр, «горячий ключ»)
--                 ~15% заказов   — customer_id IS NULL (гостевые заказы без регистрации)
--                 ~25% заказов   — равномерно по остальным 2 млн клиентов
--               order_id   — равномерный последовательный ключ (хорош для partitionColumn)
--               customer_id — перекошенный ключ (плох для partitionColumn и join)
-- =====================================================================

SELECT setseed(0.42);

-- ---------- countries ----------
CREATE TABLE countries (
    country_id   int PRIMARY KEY,
    country_name text NOT NULL,
    region       text NOT NULL
);
INSERT INTO countries
SELECT g,
       'Country_' || lpad(g::text, 3, '0'),
       (ARRAY['Europe','Asia','America','Africa','Oceania'])[1 + (g % 5)]
FROM generate_series(1, 200) g;

-- ---------- products ----------
CREATE TABLE products (
    product_id   int PRIMARY KEY,
    product_name text NOT NULL,
    category     text NOT NULL,
    price        numeric(10,2) NOT NULL
);
INSERT INTO products
SELECT g,
       'Product_' || g,
       (ARRAY['electronics','books','clothes','home','sport','toys','beauty','food',
              'garden','auto','pets','music','office','health','kids','shoes',
              'jewelry','tools','games','travel'])[1 + (g % 20)],
       round((10 + random() * 990)::numeric, 2)
FROM generate_series(1, 50000) g;

-- ---------- customers ----------
CREATE TABLE customers (
    customer_id   int PRIMARY KEY,
    full_name     text NOT NULL,
    email         text NOT NULL,
    country_id    int  NOT NULL,
    segment       text NOT NULL,
    registered_at date NOT NULL
);
INSERT INTO customers
SELECT g,
       'Customer ' || g,
       'customer' || g || '@example.com',
       1 + floor(random() * 200)::int,
       CASE WHEN random() < 0.80 THEN 'b2c' WHEN random() < 0.75 THEN 'b2b' ELSE 'vip' END,
       date '2018-01-01' + floor(random() * 2500)::int
FROM generate_series(1, 2000000) g;

UPDATE customers
SET full_name = 'BigWholesale LLC (оптовый партнёр — горячий ключ)', segment = 'b2b'
WHERE customer_id = 1;

-- ---------- orders (с перекосом) ----------
CREATE TABLE orders (
    order_id    bigint        NOT NULL,
    customer_id int,                        -- NULL = гостевой заказ
    product_id  int           NOT NULL,
    country_id  int           NOT NULL,
    quantity    int           NOT NULL,
    amount      numeric(12,2) NOT NULL,
    status      text          NOT NULL,
    created_at  timestamp     NOT NULL
);

INSERT INTO orders
SELECT g,
       CASE WHEN r < 0.60 THEN 1
            WHEN r < 0.75 THEN NULL
            ELSE 2 + floor(random() * 1999999)::int END,
       1 + floor(random() * 50000)::int,
       1 + floor(random() * 200)::int,
       1 + floor(random() * 5)::int,
       round((50 + random() * 9950)::numeric, 2),
       (ARRAY['created','paid','shipped','delivered','cancelled'])[1 + floor(random() * 5)::int],
       timestamp '2024-01-01' + random() * interval '730 days'
FROM (SELECT g, random() AS r FROM generate_series(1, 10000000) g) s;

ALTER TABLE orders ADD PRIMARY KEY (order_id);
CREATE INDEX orders_created_at_idx  ON orders (created_at);
CREATE INDEX orders_customer_id_idx ON orders (customer_id);

-- Таблица-приёмник для демо записи из Spark в PostgreSQL
CREATE TABLE customer_revenue (
    customer_id   int,
    orders_cnt    bigint,
    revenue       numeric(18,2),
    last_order_at timestamp
);

ANALYZE;
