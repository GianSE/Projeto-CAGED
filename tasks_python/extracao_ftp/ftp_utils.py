"""
Cliente FTP resiliente para o servidor do MTE (ftp.mtps.gov.br).

Particularidades desse servidor que este módulo trata:
  - Nomes de arquivo/pasta vêm em Latin-1 (acentos quebram o ftplib padrão).
  - Conexões caem com frequência em downloads longos (arquivos de vários GB).
  - Suporta REST, então dá para retomar um download interrompido.
"""
import socket
import time
from ftplib import FTP, error_perm
from pathlib import Path

from extracao_ftp.config_extracao import (
    FTP_ENCODING,
    FTP_HOST,
    FTP_MAX_RETRIES,
    FTP_TIMEOUT,
)


def _fmt_bytes(n: float) -> str:
    for unidade in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unidade}"
        n /= 1024
    return f"{n:.1f} PB"


class ClienteFTP:
    """Wrapper do ftplib com reconexão automática e download retomável."""

    def __init__(self, host: str = FTP_HOST, timeout: int = FTP_TIMEOUT):
        self.host = host
        self.timeout = timeout
        self.ftp: FTP | None = None

    # ------------------------------------------------------------------ conexão
    def conectar(self) -> FTP:
        self.fechar()
        socket.setdefaulttimeout(self.timeout)
        ftp = FTP(self.host, timeout=self.timeout)
        ftp.encoding = FTP_ENCODING
        ftp.login()  # anônimo
        ftp.set_pasv(True)
        ftp.sendcmd("TYPE I")  # binário: necessário para SIZE/REST funcionarem
        self.ftp = ftp
        return ftp

    def garantir_conexao(self) -> FTP:
        """Devolve uma conexão viva, reconectando se o servidor derrubou."""
        if self.ftp is None:
            return self.conectar()
        try:
            self.ftp.voidcmd("NOOP")
            return self.ftp
        except Exception:
            return self.conectar()

    def fechar(self) -> None:
        if self.ftp is not None:
            try:
                self.ftp.quit()
            except Exception:
                try:
                    self.ftp.close()
                except Exception:
                    pass
            self.ftp = None

    def __enter__(self):
        self.conectar()
        return self

    def __exit__(self, *_exc):
        self.fechar()

    # ---------------------------------------------------------------- listagem
    def listar(self, caminho: str) -> list[str]:
        """Lista os nomes dentro de um diretório. Devolve [] se não existir."""
        for tentativa in range(1, FTP_MAX_RETRIES + 1):
            try:
                ftp = self.garantir_conexao()
                ftp.cwd(caminho)
                return sorted(ftp.nlst())
            except error_perm:
                return []  # diretório inexistente / sem permissão
            except Exception as e:
                if tentativa == FTP_MAX_RETRIES:
                    print(f"      ⚠️  Falha ao listar {caminho}: {e}")
                    return []
                time.sleep(2 * tentativa)
                self.conectar()
        return []

    def tamanho(self, caminho: str) -> int:
        """Tamanho do arquivo remoto em bytes. 0 quando é diretório."""
        try:
            ftp = self.garantir_conexao()
            return ftp.size(caminho) or 0
        except Exception:
            return 0

    def eh_diretorio(self, caminho: str) -> bool:
        try:
            ftp = self.garantir_conexao()
            atual = ftp.pwd()
            ftp.cwd(caminho)
            ftp.cwd(atual)
            return True
        except Exception:
            return False

    # ---------------------------------------------------------------- download
    def baixar(self, caminho_remoto: str, destino: Path, tamanho_esperado: int = 0) -> bool:
        """
        Baixa um arquivo com retomada automática (REST) e múltiplas tentativas.

        Se o destino já existir com o tamanho esperado, não baixa de novo.
        Retorna True em caso de sucesso.
        """
        destino.parent.mkdir(parents=True, exist_ok=True)
        parcial = destino.with_suffix(destino.suffix + ".parcial")

        if tamanho_esperado == 0:
            tamanho_esperado = self.tamanho(caminho_remoto)

        # Já baixado numa execução anterior?
        if destino.exists() and tamanho_esperado and destino.stat().st_size == tamanho_esperado:
            print(f"      ⏭️  Já baixado ({_fmt_bytes(tamanho_esperado)})")
            return True

        for tentativa in range(1, FTP_MAX_RETRIES + 1):
            offset = parcial.stat().st_size if parcial.exists() else 0

            # Passou do esperado => arquivo parcial corrompido, começa do zero
            if tamanho_esperado and offset > tamanho_esperado:
                parcial.unlink(missing_ok=True)
                offset = 0

            if tamanho_esperado and offset == tamanho_esperado:
                break  # já está completo, só falta renomear

            try:
                ftp = self.garantir_conexao()
                inicio = time.time()
                baixado = offset
                ultimo_log = [time.time()]

                def _escrever(bloco: bytes, _f=None):
                    nonlocal baixado
                    _f.write(bloco)
                    baixado += len(bloco)
                    agora = time.time()
                    if agora - ultimo_log[0] >= 3:
                        ultimo_log[0] = agora
                        vel = (baixado - offset) / max(agora - inicio, 0.001)
                        if tamanho_esperado:
                            pct = baixado / tamanho_esperado * 100
                            print(
                                f"\r      ⬇️  {_fmt_bytes(baixado)}/{_fmt_bytes(tamanho_esperado)}"
                                f" ({pct:.1f}%) — {_fmt_bytes(vel)}/s   ",
                                end="",
                                flush=True,
                            )
                        else:
                            print(
                                f"\r      ⬇️  {_fmt_bytes(baixado)} — {_fmt_bytes(vel)}/s   ",
                                end="",
                                flush=True,
                            )

                modo = "ab" if offset else "wb"
                with open(parcial, modo) as f:
                    ftp.retrbinary(
                        f"RETR {caminho_remoto}",
                        lambda b: _escrever(b, f),
                        blocksize=1024 * 256,
                        rest=offset or None,
                    )

                decorrido = max(time.time() - inicio, 0.001)
                vel_media = (baixado - offset) / decorrido
                print(
                    f"\r      ⬇️  {_fmt_bytes(baixado)} concluído em "
                    f"{decorrido:.0f}s ({_fmt_bytes(vel_media)}/s)          "
                )
                break

            except Exception as e:
                print(f"\n      ⚠️  Tentativa {tentativa}/{FTP_MAX_RETRIES} falhou: {e}")
                self.conectar()
                if tentativa == FTP_MAX_RETRIES:
                    print(f"      ❌ Desisti de {caminho_remoto}")
                    return False
                time.sleep(3 * tentativa)

        if not parcial.exists():
            return False

        # Validação final de integridade pelo tamanho
        if tamanho_esperado and parcial.stat().st_size != tamanho_esperado:
            print(
                f"      ❌ Tamanho divergente: {parcial.stat().st_size} != {tamanho_esperado}"
            )
            return False

        parcial.replace(destino)
        return True
