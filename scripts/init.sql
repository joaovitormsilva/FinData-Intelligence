CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE bronze.precos_historicos_ativos_crypto(
    date DATE,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    volume numeric,
    dgt_crrnc_cd  VARCHAR(10),
    mrkt_cd  VARCHAR(10),
    CONSTRAINT primary_key PRIMARY KEY(dgt_crrnc_cd,date,mrkt_cd)
)PARTITION BY RANGE(date);

CREATE TABLE bronze.stg_precos_historicos_ativos_crypto(
    date DATE,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    volume numeric,
    dgt_crrnc_cd  VARCHAR(10),
    mrkt_cd  VARCHAR(10),
    CONSTRAINT stg_primary_key PRIMARY KEY(date,dgt_crrnc_cd,mrkt_cd)
);

CREATE OR REPLACE PROCEDURE bronze.sp_load_historico_cryto(p_retention_days INT DEFAULT 30)
LANGUAGE plpgsql
AS $$
DECLARE 
    v_date DATE;
    v_partition_name TEXT;
    v_count INT;
    v_old_partition_name TEXT;
    v_old_date DATE;
BEGIN
    -- 1. Verifica se há dados (Referência explícita ao schema bronze)
    SELECT COUNT(*) INTO v_count FROM bronze.stg_precos_historicos_ativos_crypto;
    IF v_count = 0 THEN
        RAISE NOTICE 'No data in staging table to load';
        RETURN;
    END IF;
    
    FOR v_date IN 
        SELECT DISTINCT date FROM bronze.stg_precos_historicos_ativos_crypto ORDER BY date ASC
    LOOP
        -- Nome da partição
        v_partition_name := format('precos_historicos_crypto_%s', to_char(v_date,'YYYY_MM_DD'));
        
        -- 2. Verificação de existência focada no schema bronze
        IF EXISTS (
            SELECT 1 FROM pg_tables 
            WHERE schemaname = 'bronze' AND tablename = v_partition_name
        ) THEN
            EXECUTE format('ALTER TABLE bronze.precos_historicos_ativos_crypto DETACH PARTITION bronze.%I', v_partition_name);
            EXECUTE format('DROP TABLE IF EXISTS bronze.%I', v_partition_name);
            RAISE NOTICE 'Partition bronze.% detached and dropped', v_partition_name;
        END IF;

        -- 3. Criação da partição garantindo o schema bronze
        EXECUTE format(
            'CREATE TABLE bronze.%I PARTITION OF bronze.precos_historicos_ativos_crypto
            FOR VALUES FROM (%L) TO (%L)',
            v_partition_name, v_date, v_date + 1
        );
        RAISE NOTICE 'Partition bronze.% created', v_partition_name;

        -- 4. Inserção
        INSERT INTO bronze.precos_historicos_ativos_crypto (date, open, high, low, close, volume, dgt_crrnc_cd, mrkt_cd)
        SELECT date, open, high, low, close, volume, dgt_crrnc_cd, mrkt_cd
        FROM bronze.stg_precos_historicos_ativos_crypto 
        WHERE date = v_date;

        GET DIAGNOSTICS v_count = ROW_COUNT;
        RAISE NOTICE 'Loaded % records for date %', v_count, v_date;
        
    END LOOP;

    -- 5. Truncate e Limpeza
    TRUNCATE TABLE bronze.stg_precos_historicos_ativos_crypto;

    FOR v_old_date IN
        SELECT DISTINCT date FROM bronze.precos_historicos_ativos_crypto
        WHERE date < CURRENT_DATE - p_retention_days
    LOOP
        v_old_partition_name := format('precos_historicos_crypto_%s', to_char(v_old_date, 'YYYY_MM_DD'));
        BEGIN
            EXECUTE format('ALTER TABLE bronze.precos_historicos_ativos_crypto DETACH PARTITION bronze.%I', v_old_partition_name);
            EXECUTE format('DROP TABLE IF EXISTS bronze.%I', v_old_partition_name);
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Failed to remove old partition %', v_old_partition_name;
        END;
    END LOOP;
    
END;
$$;