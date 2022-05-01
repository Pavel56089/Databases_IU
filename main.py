import random


def make_indexes(cur):
    ind_hash = "CREATE INDEX customers_name_index_hash ON customers USING hash(name)"
    ind_gist = "CREATE INDEX customers_name_index_gist ON customers USING gist(id)"
    ind_gin = "CREATE INDEX customers_name_index_gin ON customers USING gin(review)"
    ind_brin = "CREATE INDEX customers_name_index_brin ON customers USING brin(address)"
    cur.execute("CREATE EXTENSION btree_gist;")
    cur.execute("CREATE EXTENSION pg_trgm;")
    cur.execute("CREATE EXTENSION btree_gin;")
    cur.execute(ind_hash)
    cur.execute(ind_gist)
    cur.execute(ind_gin)
    cur.execute(ind_brin)

def make_fake_generate(cur):
    for i in range(3, 1000000):
        req = "INSERT INTO customers(id, name, address, review) VALUES (" + str(i) +", '" + fake.name() + "', '" + fake.address()[:49] +"', '" + fake.text()[:100] +"')"
        cur.execute(req)
        print(i)

def delete_indexes(cur):
    str = '''
    CREATE OR REPLACE FUNCTION drop_all_indexes() RETURNS TABLE ("order" INT, "name" TEXT) AS $$
    DECLARE i RECORD; cnt INTEGER;
    BEGIN 
        cnt = 0;
        FOR i IN ( SELECT indexname FROM get_user_indexes())
        LOOP 
            EXECUTE format('DROP INDEX IF EXISTS %I CASCADE;', i.indexname);
            "order" := cnt;
            "name" := i.indexname;
            RETURN NEXT;
            cnt = cnt + 1;    
        END LOOP;
    END;
    $$ LANGUAGE plpgsql;
    '''
    cur.execute(str)
    str = '''
    CREATE OR REPLACE FUNCTION get_user_indexes() RETURNS TABLE ("tablename" NAME, "indexname" NAME, "indexdef" TEXT) AS $$
    BEGIN 
        RETURN QUERY
        SELECT pg_i.tablename,  pg_i.indexname,  pg_i.indexdef
        FROM pg_indexes pg_i
        WHERE schemaname = 'public'
            AND pg_i.tablename !~~ 'pg%'
            AND pg_i.indexname !~~ '%_pkey'
            AND pg_i.indexname !~~ 'idx_%'
            AND pg_i.indexname !~~ '%_idx';
    END;
    $$ LANGUAGE plpgsql;
    '''
    cur.execute(str)
    str = '''
    SELECT format('Dropped: %s)  %s', "order", "name" )
    FROM drop_all_indexes()
    ORDER BY "order";
    '''
    cur.execute(str)

import psycopg2
from faker import Faker
import time
# Connect to your postgres DB
conn = psycopg2.connect("dbname=bd_ass2")

# Open a cursor to perform database operations
cur = conn.cursor()

# Execute a query
#cur.execute("CREATE TABLE customers( id int NOT NULL, name char(50), address char(50), review text)")
#cur.execute("CREATE TABLE purchases( id int NOT NULL, customer_id int NOT NULL)")
#cur.execute("CREATE TABLE purchases_products_list( id int NOT NULL, purchases_id int NOT NULL, product_id int NOT NULL)")
# cur.execute("DROP table products")
# cur.execute("DROP table sales")
# cur.execute("CREATE TABLE products( id int NOT NULL, name char(50), details text, price int, type int)")
# cur.execute("CREATE TABLE sales( id int NOT NULL, type int, discount int)")
#cur.execute("INSERT INTO customers(id, name, address, review) VALUES (1, 'Pavel', 'Innopolis', 'lalalalla')")
fake = Faker()
# for i in range (1000, 100000):
#     req = "INSERT INTO purchases(id, customer_id) VALUES (" + str(
#         i) + ", '" + str(random.randint(0, 1000000)) + "')"
#     print(req)
#     cur.execute(req)

# for i in range (0, 100000):
#     req = "INSERT INTO purchases_products_list(id, purchases_id, product_id) VALUES (" + str(
#         i) + ", '" + str(random.randint(0, 100000)) + "', '" + str(random.randint(0, 10000))  + "')"
#     print(req)
#     cur.execute(req)
# cur.execute("DROP TABLE sales")
# for i in range (0, 100000):
#     req = "INSERT INTO products(id, name, details, price, type) VALUES (" + str(
#         i) + ", '" + fake.text()[:20] + "', '" + fake.text()[:50] + "', '" + str(random.randint(3, 1000)) + "', " + str(random.randint(1, 20)) + ")"
#     print(req)
#     cur.execute(req)
#cur.execute("DROP TABLE sales")
# for i in range (0, 10):
#     req = "INSERT INTO sales(id, type, discount) VALUES (" + str(i) + ", '" + str(random.randint(1, 20)) + "', '" + str(random.randint(0, 100))  + "')"
#     print(req)
#     cur.execute(req)
###Customers with products with discounts
customer_id = 45384
cur.execute("CREATE INDEX on products(type)")
cur.execute("CREATE INDEX on sales(type)")
str_req = '''
EXPLAIN ANALYZE
SELECT products.name, products.type, products.price, sales.discount , (products.price * (100 - sales.discount)/100) FROM customers
join purchases on customers.id = purchases.customer_id
join purchases_products_list on purchases.id = purchases_products_list.purchases_id
join products on purchases_products_list.product_id = products.id
join sales on products.type = sales.type
where customers.id = 45384'''
cur.execute(str_req)
str_req2 = '''
EXPLAIN ANALYZE
SELECT customers.name, SUM(products.price), SUM(products.price * (100 - sales.discount)/100) FROM customers
join purchases on customers.id = purchases.customer_id
join purchases_products_list on purchases.id = purchases_products_list.purchases_id
join products on purchases_products_list.product_id = products.id
join sales on products.type = sales.type
group by customers.name
'''
cur.execute(str_req2)
# cur.execute("SELECT * FROM sales")

# make_fake_generate(cur)
#make_indexes(cur)
#delete_indexes(cur)

# start = time.time()
# cur.execute("EXPLAIN ANALYZE SELECT * FROM customers WHERE (review LIKE 'A%') ORDER BY name;")
# end = time.time()
# print(end - start)
# # Retrieve query results
records = cur.fetchall()
print(records)
conn.commit()