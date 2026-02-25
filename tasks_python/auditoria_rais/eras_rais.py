import pandas as pd

def normalizar_dna(texto):
    """Ordena as colunas para agrupar esquemas idênticos."""
    if pd.isna(texto) or str(texto).strip() == "" or "VINCULOS" in str(texto):
        return None
    # Limpa, coloca em minúsculo e ordena alfabeticamente
    lista = sorted([c.strip().lower() for c in str(texto).split(',') if c.strip()])
    return ", ".join(lista)

try:
    # 1. Carrega o CSV SEM cabeçalho para não haver confusão de nomes
    # Usamos header=None e pulamos a primeira linha (que é o cabeçalho errado)
    df = pd.read_csv("auditoria_arquivos_detalhada.csv", sep=';', header=None, skiprows=1, on_bad_lines='skip')
    
    # 2. MAPEAMENTO POR POSIÇÃO (Baseado no seu exemplo real)
    # Coluna 0: Caminho S3
    # Coluna 1: ANO (1985, 2017...)
    # Coluna 7: LISTA DE COLUNAS (DNA)
    
    col_path = 0
    col_ano = 1
    col_lista = 7

    print(f"🚀 Analisando {len(df)} arquivos por posição...")

    # 3. Criar a chave de DNA (agrupador)
    df['DNA_CHAVE'] = df[col_lista].apply(normalizar_dna)
    
    # Remove linhas onde a lista de colunas falhou ou veio vazia
    df = df.dropna(subset=['DNA_CHAVE'])

    # 4. AGRUPAMENTO
    agrupamentos = df.groupby('DNA_CHAVE').agg(
        Anos=(col_ano, lambda x: sorted(list(set(x.astype(str).unique())))),
        Total_Arquivos=(col_path, 'count'),
        Qtd_Colunas=(col_lista, lambda x: len(str(x.iloc[0]).split(',')))
    ).reset_index()

    # Cria coluna para ordenação (pega o primeiro ano da lista)
    agrupamentos['Primeiro_Ano'] = agrupamentos['Anos'].apply(lambda x: x[0] if x else "9999")
    agrupamentos = agrupamentos.sort_values('Primeiro_Ano')

    # Reordenar para o DNA gigante ficar no final do CSV
    agrupamentos = agrupamentos[['Primeiro_Ano', 'Anos', 'Total_Arquivos', 'Qtd_Colunas', 'DNA_CHAVE']]

    # 5. RESULTADO NO TERMINAL
    print("="*80)
    print(f"📊 RELATÓRIO: {len(agrupamentos)} ARQUITETURAS ÚNICAS ENCONTRADAS")
    print("="*80)

    for i, row in agrupamentos.iterrows():
        anos_str = ", ".join(row['Anos'])
        print(f"🏗️ ARQUITETURA #{i+1}")
        print(f"   📅 Anos: {anos_str}")
        print(f"   📏 Colunas: {row['Qtd_Colunas']} | 📄 Arquivos: {row['Total_Arquivos']}")
        print(f"   🧪 DNA (início): {row['DNA_CHAVE'][:70]}...")
        print("-" * 50)

    # 6. Salva o CSV final
    agrupamentos.to_csv("mapeamento_final_agrupado.csv", sep=';', index=False)
    print("\n✅ Mapa final (COM ANOS CORRETOS) salvo em 'mapeamento_final_agrupado.csv'")

except Exception as e:
    print(f"❌ Erro ao processar: {e}")