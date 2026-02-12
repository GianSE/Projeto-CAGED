def sql_gerar_hash_id(colunas, alias=None):
    """
    Gera Hash ID como VARCHAR(16).
    - Seguro contra arredondamento do Power BI.
    - Metade do tamanho do MD5 tradicional.
    - Zero risco de Overflow.
    """
    # 1. Limpeza (Sua lógica padrão)
    cols_safe = [f"upper(coalesce(nullif(trim(CAST(\"{c}\" AS VARCHAR)), ''), 'ND'))" for c in colunas]
    cols_str = ", ".join(cols_safe)
    
    # 2. Gera MD5 e corta os primeiros 16 caracteres
    # Retorna TEXTO, não número.
    sql = f"CAST(substr(md5(concat_ws('_', {cols_str})), 1, 16) AS VARCHAR(16))"
    
    if alias:
        return f"{sql} AS {alias}"
    return sql