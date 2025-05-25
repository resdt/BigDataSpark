DO $$
DECLARE
    file_path TEXT;
    file_name TEXT;
    files TEXT[] := ARRAY[
        'MOCK_DATA.csv',
        'MOCK_DATA (1).csv',
        'MOCK_DATA (2).csv',
        'MOCK_DATA (3).csv',
        'MOCK_DATA (4).csv',
        'MOCK_DATA (5).csv',
        'MOCK_DATA (6).csv',
        'MOCK_DATA (7).csv',
        'MOCK_DATA (8).csv',
        'MOCK_DATA (9).csv'
    ];
    files_loaded INT := 0;
BEGIN
    -- Create the target table with SERIAL primary key
    DROP TABLE IF EXISTS mock;

    CREATE TABLE mock (
        id SERIAL PRIMARY KEY,
        customer_first_name VARCHAR(100),
        customer_last_name VARCHAR(100),
        customer_age INTEGER,
        customer_email VARCHAR(100),
        customer_country VARCHAR(100),
        customer_postal_code VARCHAR(20),
        customer_pet_type VARCHAR(50),
        customer_pet_name VARCHAR(100),
        customer_pet_breed VARCHAR(100),
        seller_first_name VARCHAR(100),
        seller_last_name VARCHAR(100),
        seller_email VARCHAR(100),
        seller_country VARCHAR(100),
        seller_postal_code VARCHAR(20),
        product_name VARCHAR(100),
        product_category VARCHAR(100),
        product_price DECIMAL(10, 2),
        product_quantity INTEGER,
        sale_date DATE,
        sale_customer_id INTEGER,
        sale_seller_id INTEGER,
        sale_product_id INTEGER,
        sale_quantity INTEGER,
        sale_total_price DECIMAL(10, 2),
        store_name VARCHAR(100),
        store_location VARCHAR(100),
        store_city VARCHAR(100),
        store_state VARCHAR(100),
        store_country VARCHAR(100),
        store_phone VARCHAR(20),
        store_email VARCHAR(100),
        pet_category VARCHAR(50),
        product_weight DECIMAL(10, 2),
        product_color VARCHAR(50),
        product_size VARCHAR(20),
        product_brand VARCHAR(100),
        product_material VARCHAR(100),
        product_description TEXT,
        product_rating DECIMAL(2, 1),
        product_reviews INTEGER,
        product_release_date DATE,
        product_expiry_date DATE,
        supplier_name VARCHAR(100),
        supplier_contact VARCHAR(100),
        supplier_email VARCHAR(100),
        supplier_phone VARCHAR(20),
        supplier_address TEXT,
        supplier_city VARCHAR(100),
        supplier_country VARCHAR(100)
    );

    -- Create temporary import table (matches CSV structure including original ID)
    CREATE TEMP TABLE temp_import (
        original_id INTEGER,  -- This is the ID from your CSV files
        customer_first_name VARCHAR(100),
        customer_last_name VARCHAR(100),
        customer_age INTEGER,
        customer_email VARCHAR(100),
        customer_country VARCHAR(100),
        customer_postal_code VARCHAR(20),
        customer_pet_type VARCHAR(50),
        customer_pet_name VARCHAR(100),
        customer_pet_breed VARCHAR(100),
        seller_first_name VARCHAR(100),
        seller_last_name VARCHAR(100),
        seller_email VARCHAR(100),
        seller_country VARCHAR(100),
        seller_postal_code VARCHAR(20),
        product_name VARCHAR(100),
        product_category VARCHAR(100),
        product_price DECIMAL(10, 2),
        product_quantity INTEGER,
        sale_date DATE,
        sale_customer_id INTEGER,
        sale_seller_id INTEGER,
        sale_product_id INTEGER,
        sale_quantity INTEGER,
        sale_total_price DECIMAL(10, 2),
        store_name VARCHAR(100),
        store_location VARCHAR(100),
        store_city VARCHAR(100),
        store_state VARCHAR(100),
        store_country VARCHAR(100),
        store_phone VARCHAR(20),
        store_email VARCHAR(100),
        pet_category VARCHAR(50),
        product_weight DECIMAL(10, 2),
        product_color VARCHAR(50),
        product_size VARCHAR(20),
        product_brand VARCHAR(100),
        product_material VARCHAR(100),
        product_description TEXT,
        product_rating DECIMAL(2, 1),
        product_reviews INTEGER,
        product_release_date DATE,
        product_expiry_date DATE,
        supplier_name VARCHAR(100),
        supplier_contact VARCHAR(100),
        supplier_email VARCHAR(100),
        supplier_phone VARCHAR(20),
        supplier_address TEXT,
        supplier_city VARCHAR(100),
        supplier_country VARCHAR(100)
    );

    RAISE NOTICE 'Tables created successfully';

    -- Import each CSV file into temp table, then insert into main table
    FOREACH file_name IN ARRAY files LOOP
        file_path := '/data/' || file_name;
        
        BEGIN
            -- Import ALL data from CSV (including original ID)
            EXECUTE format('COPY temp_import FROM %L WITH (FORMAT CSV, HEADER)', file_path);

            -- Insert into final table, skipping the original ID (letting SERIAL generate new IDs)
            INSERT INTO
                mock (
                    customer_first_name,
                    customer_last_name,
                    customer_age,
                    customer_email,
                    customer_country,
                    customer_postal_code,
                    customer_pet_type,
                    customer_pet_name,
                    customer_pet_breed,
                    seller_first_name,
                    seller_last_name,
                    seller_email,
                    seller_country,
                    seller_postal_code,
                    product_name,
                    product_category,
                    product_price,
                    product_quantity,
                    sale_date,
                    sale_customer_id,
                    sale_seller_id,
                    sale_product_id,
                    sale_quantity,
                    sale_total_price,
                    store_name,
                    store_location,
                    store_city,
                    store_state,
                    store_country,
                    store_phone,
                    store_email,
                    pet_category,
                    product_weight,
                    product_color,
                    product_size,
                    product_brand,
                    product_material,
                    product_description,
                    product_rating,
                    product_reviews,
                    product_release_date,
                    product_expiry_date,
                    supplier_name,
                    supplier_contact,
                    supplier_email,
                    supplier_phone,
                    supplier_address,
                    supplier_city,
                    supplier_country
                )
            SELECT
                customer_first_name,
                customer_last_name,
                customer_age,
                customer_email,
                customer_country,
                customer_postal_code,
                customer_pet_type,
                customer_pet_name,
                customer_pet_breed,
                seller_first_name,
                seller_last_name,
                seller_email,
                seller_country,
                seller_postal_code,
                product_name,
                product_category,
                product_price,
                product_quantity,
                sale_date,
                sale_customer_id,
                sale_seller_id,
                sale_product_id,
                sale_quantity,
                sale_total_price,
                store_name,
                store_location,
                store_city,
                store_state,
                store_country,
                store_phone,
                store_email,
                pet_category,
                product_weight,
                product_color,
                product_size,
                product_brand,
                product_material,
                product_description,
                product_rating,
                product_reviews,
                product_release_date,
                product_expiry_date,
                supplier_name,
                supplier_contact,
                supplier_email,
                supplier_phone,
                supplier_address,
                supplier_city,
                supplier_country
            FROM temp_import;
                        
            -- Clear temp table for next file
            TRUNCATE temp_import;
            
            files_loaded := files_loaded + 1;
            RAISE NOTICE 'Successfully imported %', file_name;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING 'Failed to import %: %', file_name, SQLERRM;
        END;
    END LOOP;

    -- Clean up
    DROP TABLE temp_import;
    
    RAISE NOTICE 'Import process completed. Successfully loaded % out of % files',
        files_loaded, array_length(files, 1);

    -- Создание таблиц измерений
    DROP TABLE IF EXISTS dim_customers CASCADE;
    CREATE TABLE dim_customers (
        customer_id SERIAL PRIMARY KEY,
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        age INTEGER,
        email VARCHAR(100) UNIQUE,
        country VARCHAR(100),
        postal_code VARCHAR(20),
        pet_type VARCHAR(50),
        pet_name VARCHAR(100),
        pet_breed VARCHAR(100)
    );

    DROP TABLE IF EXISTS dim_sellers CASCADE;
    CREATE TABLE dim_sellers (
        seller_id SERIAL PRIMARY KEY,
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        email VARCHAR(100) UNIQUE,
        country VARCHAR(100),
        postal_code VARCHAR(20)
    );

    DROP TABLE IF EXISTS dim_products CASCADE;
    CREATE TABLE dim_products (
        product_id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        category VARCHAR(100),
        price DECIMAL(10, 2),
        weight DECIMAL(10, 2),
        color VARCHAR(50),
        size VARCHAR(20),
        brand VARCHAR(100),
        material VARCHAR(100),
        description TEXT,
        rating DECIMAL(2, 1),
        reviews INTEGER,
        release_date DATE,
        expiry_date DATE,
        pet_category VARCHAR(50)
    );

    DROP TABLE IF EXISTS dim_stores CASCADE;
    CREATE TABLE dim_stores (
        store_id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        location VARCHAR(100),
        city VARCHAR(100),
        state VARCHAR(100),
        country VARCHAR(100),
        phone VARCHAR(20),
        email VARCHAR(100)
    );

    DROP TABLE IF EXISTS dim_suppliers CASCADE;
    CREATE TABLE dim_suppliers (
        supplier_id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        contact VARCHAR(100),
        email VARCHAR(100) UNIQUE,
        phone VARCHAR(20),
        address TEXT,
        city VARCHAR(100),
        country VARCHAR(100)
    );

    -- Фактическая таблица продаж
    DROP TABLE IF EXISTS fact_sales CASCADE;
    CREATE TABLE fact_sales (
        sale_id SERIAL PRIMARY KEY,
        sale_date DATE,
        quantity INTEGER,
        total_price DECIMAL(10, 2),

        customer_id INTEGER REFERENCES dim_customers(customer_id),
        seller_id INTEGER REFERENCES dim_sellers(seller_id),
        product_id INTEGER REFERENCES dim_products(product_id),
        store_id INTEGER REFERENCES dim_stores(store_id),
        supplier_id INTEGER REFERENCES dim_suppliers(supplier_id)
    );

END $$;
