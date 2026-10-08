"""
Piloto do runner do GitHub Actions: mede a latencia runner->FTP do MTE e
runner->Hugging Face, por ano, sem processar dado nenhum ainda.

POR QUE POR ANO
----------------
O desenho real do pipeline (ver gold_caged/consolidar.py e
silver_caged/construir_silver.py) processa e publica um ano por vez, para
poder reprocessar so o periodo com problema sem tocar o resto. Este piloto
testa a mecanica do matrix-por-ano do Actions antes de ligar o pipeline de
verdade nele.

Uso:
    python teste_conectividade.py --ano 2022
"""
import argparse
import ftplib
import sys
import time

FTP_HOST = "ftp.mtps.gov.br"
FTP_BASE = "/pdet/microdados"


def testar_ftp(ano: int) -> None:
    print(f"\n=== FTP ({FTP_HOST}) ===")
    t0 = time.perf_counter()
    try:
        ftp = ftplib.FTP(FTP_HOST, timeout=30)
        ftp.login()  # anonimo
        conectou = time.perf_counter() - t0

        t1 = time.perf_counter()
        pastas = ftp.nlst(FTP_BASE)
        listou = time.perf_counter() - t1
        ftp.quit()

        print(f"   conexao+login: {conectou*1000:.0f} ms")
        print(f"   listagem de {FTP_BASE}: {listou*1000:.0f} ms ({len(pastas)} entradas)")
        print(f"   OK — ano {ano} nao muda este teste (FTP nao tem o dado "
              "particionado por ano nesse nivel, so por tipo de arquivo)")
    except Exception as e:
        print(f"   FALHOU: {e!r}")
        sys.exit(1)


def testar_hf(ano: int) -> None:
    import os

    print("\n=== Hugging Face ===")
    token = os.getenv("HF_TOKEN")
    if not token:
        print("   HF_TOKEN nao configurado nos secrets do repositorio — "
              "pulando a perna de upload. Adicione em Settings > Secrets > "
              "Actions para testar essa parte tambem.")
        return

    from huggingface_hub import CommitOperationAdd, CommitOperationDelete, HfApi

    repo = "Gianpedro/ci-pilot-caged"
    caminho = f"_piloto/teste_ano_{ano}.txt"
    conteudo = f"piloto do github actions - ano {ano}\n".encode()

    api = HfApi(token=token)
    api.create_repo(repo_id=repo, repo_type="dataset", exist_ok=True, private=False)

    t0 = time.perf_counter()
    api.create_commit(
        repo_id=repo, repo_type="dataset",
        operations=[CommitOperationAdd(path_in_repo=caminho, path_or_fileobj=conteudo)],
        commit_message=f"piloto: teste de conectividade (ano {ano})",
    )
    subiu = time.perf_counter() - t0
    print(f"   upload de {len(conteudo)} bytes: {subiu*1000:.0f} ms")

    api.create_commit(
        repo_id=repo, repo_type="dataset",
        operations=[CommitOperationDelete(path_in_repo=caminho)],
        commit_message=f"piloto: limpeza do teste (ano {ano})",
    )
    print("   limpeza OK")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--ano", type=int, required=True)
    args = p.parse_args()

    print(f"Piloto de conectividade — ano {args.ano}")
    testar_ftp(args.ano)
    testar_hf(args.ano)
    return 0


if __name__ == "__main__":
    sys.exit(main())
