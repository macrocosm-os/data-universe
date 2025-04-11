CREATE OR REPLACE FUNCTION create_data_entity_partitions()
RETURNS void AS $$
DECLARE
    start_date DATE := CURRENT_DATE - INTERVAL '30 day';
    end_date DATE := CURRENT_DATE + INTERVAL '1 day';
    partition_date DATE;
    partition_name TEXT;
    date_str TEXT;
BEGIN
    -- Tạo các partition mới
    FOR partition_date IN SELECT generate_series(start_date, end_date, '1 day'::interval)::date LOOP
        -- Format tên partition
        date_str := TO_CHAR(partition_date, 'YYYYMMDD');
        partition_name := 'data_entity_' || date_str;
        
        -- Kiểm tra nếu partition chưa tồn tại
        PERFORM 1 FROM pg_class c 
        JOIN pg_namespace n ON n.oid = c.relnamespace 
        WHERE n.nspname = 'public' AND c.relname = partition_name;
        
        IF NOT FOUND THEN
            -- Tạo partition mới
            EXECUTE 'CREATE TABLE IF NOT EXISTS ' || partition_name || 
                    ' PARTITION OF DataEntity FOR VALUES FROM (''' || 
                    partition_date || ''') TO (''' || 
                    (partition_date + INTERVAL '1 day') || ''')';
            
            RAISE NOTICE 'Created partition %', partition_name;
        END IF;
    END LOOP;
    
    -- Xóa các partition cũ (quá 30 ngày)
    FOR partition_name IN 
        SELECT c.relname
        FROM pg_inherits i
        JOIN pg_class c ON i.inhrelid = c.oid
        JOIN pg_class parent ON i.inhparent = parent.oid
        WHERE parent.relname = 'dataentity'
        AND c.relname LIKE 'data_entity_%'
    LOOP
        -- Kiểm tra nếu tên phù hợp định dạng data_entity_YYYYMMDD
        IF partition_name ~ '^data_entity_[0-9]{8}$' THEN
            -- Trích xuất phần ngày tháng từ tên
            date_str := SUBSTRING(partition_name FROM 12);
            
            -- Chuyển thành date
            BEGIN
                partition_date := TO_DATE(date_str, 'YYYYMMDD');
                
                -- Kiểm tra nếu cũ hơn 30 ngày
                IF partition_date < CURRENT_DATE - INTERVAL '30 day' THEN
                    EXECUTE 'DROP TABLE IF EXISTS ' || partition_name;
                    RAISE NOTICE 'Dropped old partition %', partition_name;
                END IF;
            EXCEPTION WHEN OTHERS THEN
                RAISE NOTICE 'Could not parse date from partition: %', partition_name;
            END;
        END IF;
    END LOOP;

    RAISE NOTICE 'Partition maintenance completed';
END;
$$ LANGUAGE plpgsql;