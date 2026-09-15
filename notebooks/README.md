# Cadernos de análise — pesquisa do TCC

Cada caderno é um **capítulo de metodologia executável**: código, resultado e
argumentação juntos. Abra no Colab e rode — não precisa instalar nada, não
precisa de MinIO, Docker, nem dos 60 GB de microdados brutos.

## Abrir no Google Colab

| caderno | pergunta que responde | |
|---|---|---|
| **01 — Previsão do saldo** | O saldo de emprego em TI é previsível? Com que margem? | [![Abrir no Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/GianSE/Projeto-CAGED/blob/main/notebooks/01_previsao_saldo.ipynb) |

## Como isto se encaixa no projeto

O projeto tem quatro consumidores do mesmo dado, e cada um serve a um público:

```
microdados (FTP do MTE)
   └─ bronze → silver → silver-ti → gold          (pipeline, MinIO local)
                                      │
                    ┌─────────────────┼─────────────────┐
                    ▼                 ▼                 ▼
              dashboard          cadernos         datasets no
             (Streamlit)          (Colab)        Hugging Face
                    │                 │                 │
             quem quer            a banca          quem quer
              explorar          e o orientador     os dados
```

- O **dashboard** é o produto. Interativo, e esconde o código de propósito.
- Os **cadernos** são o método. Mostram o código e permitem conferir.

Um dashboard não deixa ninguém verificar a metodologia; um caderno não é o que
se manda para quem só quer ver o mercado de TI. São coisas diferentes.

## De onde vêm os dados

Da camada **gold** publicada: [`Gianpedro/mercado-ti-gold`](https://huggingface.co/datasets/Gianpedro/mercado-ti-gold)
— 24 tabelas, 3,3 MB, públicas, sem credencial.

São os mesmos números da camada silver, já agregados. A silver consolidada tem
110 MB **por arquivo**; a gold inteira tem 3,3 MB. Para um caderno que roda no
navegador de outra pessoa, essa diferença decide se abre em um segundo ou em
trinta.

## Uma decisão de projeto que vale explicar

Os cadernos **importam** os módulos do pipeline (`ciencia_dados.previsao_saldo`
e companhia) em vez de reimplementar a análise. O caderno clona o repositório e
chama as mesmas funções que produzem os números publicados.

O motivo: duas implementações da mesma análise divergem, e quando divergem
ninguém sabe qual está certa. Assim existe uma fonte só — se o pipeline mudar,
o caderno muda com ele.

O que o caderno acrescenta é o que o código não carrega: a narrativa, o
diagnóstico passo a passo, os gráficos de resíduo, e a justificativa de cada
escolha metodológica.

## Cadernos planejados

| tema | o que acrescenta |
|---|---|
| Nowcast do estoque | A RAIS sai com um ano de atraso; o CAGED está em dia. A relação entre os dois, validada em ano retido. |
| Sobrevivência dos vínculos | Quanto dura um emprego de TI. Inclui por que Kaplan-Meier direto devolvia 48 anos. |
| Hiato salarial | Decomposição de Oaxaca-Blinder, 19 anos. A parte explicada do hiato de gênero é negativa. |
| Recorte e limitações | Por que setor OU ocupação; o domicílio fiscal de Barueri; a conversão invertida do salário mínimo. |

## Rodando localmente

```bash
cd notebooks
../.venv/Scripts/python.exe -m pip install matplotlib jupyter
jupyter lab
```

O caderno detecta se está no Colab e ajusta o caminho do pipeline sozinho.
