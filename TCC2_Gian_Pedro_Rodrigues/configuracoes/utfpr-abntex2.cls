%%-----------------------------------------------------------------------------
%% The LaTeX Project Public License 
%%-----------------------------------------------------------------------------
%% This work may be distributed and/or modified under the
%% conditions of the LaTeX Project Public License, either version 1.3
%% of this license or (at your option) any later version.
%% The latest version of this license is in
%%   http://www.latex-project.org/lppl.txt
%% and version 1.3 or later is part of all distributions of LaTeX
%% version 2005/12/01 or later.
%%
%% Further information about abnTeX2 are available on http://www.abntex.net.br/
%%------------------------------------------------------------------------------


% -----------------------------------------------------------------------------
% Universidade TecnolÃ³gica Federal do ParanÃ¡ - UTFPR
%
% CustomizaÃ§Ã£o da classe abnTeX2 (http://www.abntex.net.br/) para as normas da UTFPR
%
% Projeto: http://tcc.tsi.gp.utfpr.edu.br/paginas/modelos-latex-da-utfpr
%
% Autores: Diego Marczal
% 	       Michael Vornes <https://github.com/mvornes>
%
%------------------------------------------------------------------------------
% CodificaÃ§Ã£o: UTF-8
% LaTeX:  abnTeX2          
% -----------------------------------------------------------------------------

\NeedsTeXFormat{LaTeX2e}
\ProvidesClass{configuracoes/utfpr-abntex2}[2016/07/07 v1.0]

% Repassa os parÃ¢metros ao abntex2
\DeclareOption*{\PassOptionsToClass{\CurrentOption}{abntex2}}

\PassOptionsToClass{%
% OpÃ§Ãµes da classe memoir
    12pt,                     % Tamanho da fonte
    a4paper,                  % Tamanho do papel.
    sumario=abnt-6027-2012,      % Seleciona o sumÃ¡rio tradicional
% OpÃ§Ãµes do pacote babel
    brazil,                   % Idioma do documento
}{abntex2}

\ProcessOptions\relax
\LoadClass{abntex2}

\RequirePackage[brazilian, hyperpageref]{backref}       % Produzir Ã­ndice nas referÃªncias
\RequirePackage{makeidx}                                % Produzir Ã­ndice remissivo de palavras
\RequirePackage{chngcntr}
\RequirePackage{graphicx}

% -----------------------------------------------------------------------------
% ConfiguraÃ§Ãµes gerais
% -----------------------------------------------------------------------------

% Retira o espaÃ§o extra desnecessÃ¡rio nas frases
\frenchspacing

% ConfiguraÃ§Ãµes do pacote backref usado sem a opÃ§Ã£o hyperpageref de backref
\renewcommand{\backrefpagesname}{Citado na(s) p\'agina(s):~}

% Texto padrÃ£o antes do nÃºmero das pÃ¡ginas
\renewcommand{\backref}{}

% Define as listas de item compactas (sem espaÃ§amento entre os itens)
\setlist{noitemsep, topsep=0pt, parsep=0pt, partopsep=0pt}

% Define os textos da citaÃ§Ã£o
\renewcommand*{\backrefalt}[4]
{%
    \ifcase #1
        Nenhuma cita\c{c}\~ao no texto.
    \or
        Citado na p\'agina #2.
    \else
        Citado #1 vezes nas p\'aginas #2.
    \fi
}

% Define a numeraÃ§Ã£o das equaÃ§Ãµes de forma sequencial
\counterwithout{equation}{chapter}

% -----------------------------------------------------------------------------
% Comandos de dados
% -----------------------------------------------------------------------------

% Comandos de dados - Departamento
\providecommand{\imprimirdepartamento}{}
\newcommand{\departamento}[1]{\renewcommand{\imprimirdepartamento}{#1}}

% Comandos de dados - Programa
\providecommand{\imprimirprograma}{}
\newcommand{\programa}[1]{\renewcommand{\imprimirprograma}{#1}}

% Comandos de dados - SubtÃ­tulo do trabalho
\providecommand{\imprimirsubtitulo}{}
\newcommand{\subtitulo}[1]{\renewcommand{\imprimirsubtitulo}{#1}}

% Comandos de dados - InstituiÃ§Ã£o do orientador
\providecommand{\imprimirinstOrientador}{}
\newcommand{\instOrientador}[1]{\renewcommand{\imprimirinstOrientador}{#1}}

% Comandos de dados - InstituiÃ§Ã£o do coorientador
\providecommand{\imprimirinstCoorientador}{}
\newcommand{\instCoorientador}[1]{\renewcommand{\imprimirinstCoorientador}{#1}}

% Comandos de dados -  Tipo de trabalho acadÃªmico (trabalho de conclusÃ£o de curso, tese, dissertaÃ§Ã£o ou projeto de qualificaÃ§Ã£o)
\providecommand{\imprimirprojeto}{}
\newcommand{\projeto}[1]{\renewcommand{\imprimirprojeto}{#1}}

% Comandos de dados - TÃ­tulo AcadÃªmico a ser alcanÃ§ado (Bacharel, TecnÃ³logo)
\providecommand{\imprimirtituloAcademico}{}
\newcommand{\tituloAcademico}[1]{\renewcommand{\imprimirtituloAcademico}{#1}}

% Comandos de dados - TÃ­tulo em InglÃªs
\providecommand{\imprimirautorcitacao}{}
\newcommand{\autorcitacao}[1]{\renewcommand{\imprimirautorcitacao}{#1}}

% Comandos de dados - Nome autor para citaÃ§Ã£o
\providecommand{\imprimirtitleabstract}{}
\newcommand{\titleabstract}[1]{\renewcommand{\imprimirtitleabstract}{#1}}

% Comandos de dados - Ãrea de concentraÃ§Ã£o com RÃ³tulo
\newcommand{\imprimirareaconcentracaoRotulo}{\'Area de concentra\c{c}\~ao: }
\providecommand{\imprimirareaconcentracao}{}
\newcommand{\areaconcentracao}[1]{\renewcommand{\imprimirareaconcentracao}{#1}}

% Comandos de dados - linha de pesquisa
\newcommand{\imprimirlinhapesquisaRotulo}{Linha de pesquisa: }
\providecommand{\imprimirlinhapesquisa}{}
\newcommand{\linhapesquisa}[1]{\renewcommand{\imprimirlinhapesquisa}{#1}}

% Comandos de dados - Texto da folha de aprovaÃ§Ã£o
\providecommand{\imprimirtextopadraofolhadeaprovacao}{}
\newcommand{\textopadraofolhadeaprovacao}[1]{\renewcommand{\imprimirtextopadraofolhadeaprovacao}{#1}}

% Comandos de dados - Logomarca da instituiÃ§Ã£o
\providecommand{\imprimirlogoinstituicao}{}
\newcommand{\logoinstituicao}[2]{\renewcommand{\imprimirlogoinstituicao}{\includegraphics[width={#1}\textwidth]{#2}}}

% -----------------------------------------------------------------------------
% Estilo de elementos
% -----------------------------------------------------------------------------

% Elementos prÃ©-textuais
\renewcommand{\pretextual}
{%
    \pagenumbering{gobble}
    \aliaspagestyle{chapter}{empty}
    \pagestyle{empty}
    \aliaspagestyle{cleared}{empty}
    \aliaspagestyle{part}{empty}
}

% Configura layout para elementos textuais
\renewcommand{\textual}
{%
    \pagenumbering{arabic} % A numeraÃ§Ã£o das pÃ¡ginas Ã© em algarismos arÃ¡bicos, inciando em 1
    \pagestyle{abntheadings}
    \aliaspagestyle{chapter}{abntchapfirst}
    \nouppercaseheads
    \bookmarksetup{startatroot}
}

% Configura layout para elementos pÃ³s-textuais
\renewcommand{\postextual}
{%
    \chapterstyle{abnt}
    \phantompart
}

% -----------------------------------------------------------------------------
% Objetos flutuantes
% -----------------------------------------------------------------------------

% Comando que trata a fonte nos objetos flutuantes
\renewcommand{\fonte}[1]
{%
    \begin{SingleSpacing}\par\end{SingleSpacing}
    \centering\small{Fonte: #1}
}

% Comando que trata as notas nos objetos flutuantes
\renewcommand{\nota}[1]
{%
    \begin{SingleSpacing}\par\end{SingleSpacing}
    \begin{tabular}{l p{.85\textwidth}}%
        Nota: & #1
    \end{tabular}
}

% -----------------------------------------------------------------------------
% ParÃ¡grafos
% -----------------------------------------------------------------------------

% Tamanho da indentaÃ§Ã£o do parÃ¡grafo
\setlength{\parindent}{1.5cm}

% EspaÃ§amento entre um parÃ¡grafo e outro
\setlength{\parskip}{0.0cm}

% -----------------------------------------------------------------------------
% Fontes
% -----------------------------------------------------------------------------

% RedefiniÃ§Ã£o de fontes
\renewcommand{\ABNTEXchapterfont}{\normalfont\bfseries}
\renewcommand{\ABNTEXchapterfontsize}{\normalsize}

\renewcommand{\ABNTEXpartfont}{\normalfont}
\renewcommand{\ABNTEXpartfontsize}{\ABNTEXchapterfontsize}

\renewcommand{\ABNTEXsectionfont}{\normalfont}
\renewcommand{\ABNTEXsectionfontsize}{\normalsize}

\renewcommand{\ABNTEXsubsectionfont}{\normalfont}
\renewcommand{\ABNTEXsubsectionfontsize}{\normalsize}

\renewcommand{\ABNTEXsubsubsectionfont}{\normalfont}
\renewcommand{\ABNTEXsubsubsectionfontsize}{\normalsize}

\renewcommand{\ABNTEXsubsubsubsectionfont}{\normalfont}
\renewcommand{\ABNTEXsubsubsubsectionfontsize}{\normalsize}

% -----------------------------------------------------------------------------
% SumÃ¡rio
% -----------------------------------------------------------------------------

% TravessÃ£o no sumÃ¡rio para apÃªndices e anexos
\renewcommand{\cftchapteraftersnum}{}

% Espaço entre itens do sumário de diferentes capítulos (alinhado ao modelo oficial UTFPR = 0.1em).
% Evita o gap grande/duplicado antes de Referências (\phantompart soma outro skip).
\setlength{\cftbeforechapterskip}{0.1em \@plus\p@}

% -----------------------------------------------------------------------------
% Listas
% -----------------------------------------------------------------------------

% Define os tÃ­tulos das listas
\addto\captionsbrazil{%
% Ajusta nomes padrÃµes do babel
    \renewcommand{\listadesiglasname}{LISTA DE ABREVIATURAS E SIGLAS}
    \renewcommand{\listadesimbolosname}{LISTA DE S\'{I}MBOLOS}
    \renewcommand{\listfigurename}{LISTA DE FIGURAS}
    \renewcommand{\listtablename}{LISTA DE TABELAS}
    \renewcommand{\indexname}{\'{I}NDICE REMISSIVO}
% Ajusta nomes usados com a macro \autoref
    \renewcommand{\pageautorefname}{p\'agina}
    \renewcommand{\sectionautorefname}{Se\c{c}\~ao}
    \renewcommand{\subsectionautorefname}{Subse\c{c}\~ao}
    \renewcommand{\subsubsectionautorefname}{Subsubse\c{c}\~ao}
    \renewcommand{\paragraphautorefname}{Subse\c{c}\~ao}
}

% Cria a lista de quadros
\newcommand{\listofquadrosname}{Lista de Quadros}
\newcommand{\quadroname}{Quadro}
\newfloat[chapter]{quadro}{loq}{\quadroname}
\newlistof{listofquadros}{loq}{\listofquadrosname}
\newlistentry{quadro}{loq}{0}
\counterwithout{quadro}{chapter}
\renewcommand{\cftquadroname}{\quadroname\space}
\renewcommand*{\cftquadroaftersnum}{\hfill--\hfill}

% -----------------------------------------------------------------------------
% Capa
% -----------------------------------------------------------------------------
\makeatletter
\renewcommand{\imprimircapa}{
    \begin{capa}
        \begin{center}
            \imprimirlogoinstituicao\\
            \vspace{0.5cm}
		\begin{SingleSpacing}
            		\large\MakeUppercase{\imprimirinstituicao} \\
	    		\abntex@ifnotempty{\imprimirdepartamento}
            		{
                		\MakeUppercase{\imprimirdepartamento} \\
            		}
           		\large\MakeUppercase{\imprimirprograma} \\
		\end{SingleSpacing}
        \end{center}
        \vfill
	\begin{center}
		\large\MakeUppercase{\imprimirautor}\\
	\end{center}
	\vspace{1.5cm}
        \begin{center}
            \ABNTEXchapterfont\large\MakeUppercase{\imprimirtitulo}
            \abntex@ifnotempty{\imprimirsubtitulo}
	    {%
                {\ABNTEXchapterfont: }{\large{\imprimirsubtitulo}}
            }
        \end{center}
	\vspace{1.5cm}
	\begin{center}
		\large\MakeUppercase{\imprimirprojeto}
	\end{center}
	\vfill
        \begin{center}
		\begin{SingleSpacing}
    		\large\MakeUppercase{\imprimirlocal}\\
    		\large\MakeUppercase{\imprimirdata}
		\end{SingleSpacing}
    	\end{center}

    \end{capa}

}
\makeatother

% -----------------------------------------------------------------------------
% Folha de rosto
% -----------------------------------------------------------------------------
\makeatletter
% \renewcommand{\folhaderostocontent}
\renewcommand{\imprimirfolhaderosto}
{%
    \setcounter{page}{2}
    \begin{center}
        \large\MakeUppercase{\imprimirautor}\\
    \end{center}

    \vspace*{96pt}

    \begin{center}
        \ABNTEXchapterfont\large\MakeUppercase{\imprimirtitulo}
        \abntex@ifnotempty{\imprimirsubtitulo}
	{%
        	{\ABNTEXchapterfont: }{\large{\imprimirsubtitulo}}
        }
    \end{center}

    \vspace*{60pt}

    \abntex@ifnotempty{\imprimirpreambulo}
    {%
        \SingleSpacing
        \begin{tabular}{p{.25\textwidth}p{.13\textwidth}p{.44\textwidth}}
            & \multicolumn{2}{p{.6\textwidth}}{\small\hyphenpenalty=10000{\imprimirpreambulo}} \\ & & \\
            \abntex@ifnotempty{\imprimirareaconcentracao}
            {%
                & \multicolumn{2}{p{.6\textwidth}}{\small\hyphenpenalty=10000{\imprimirareaconcentracaoRotulo\imprimirareaconcentracao}} \\ & & \\
            }
            \abntex@ifnotempty{\imprimirlinhapesquisa}
            {%
                & \multicolumn{2}{p{.6\textwidth}}{\small\hyphenpenalty=10000{\imprimirlinhapesquisaRotulo\imprimirlinhapesquisa}} \\ & & \\
            }
            \abntex@ifnotempty{\imprimirorientador}
            {%
                & \small\imprimirorientadorRotulo & \imprimirorientador \\
                & & \small\imprimirinstOrientador \\ & & \\
            }
            \abntex@ifnotempty{\imprimircoorientador}
            {%
                & \small\imprimircoorientadorRotulo & \imprimircoorientador \\
                & & \small\imprimirinstCoorientador
            }
        \end{tabular}
    }

    \vspace*{\fill}

    \begin{center}
	\begin{SingleSpacing}
            \large\MakeUppercase{\imprimirlocal}\\
            \large\MakeUppercase{\imprimirdata}\\
            
            \vspace*{0.3cm}
            \begin{table}[!h]
            \begin{tabular}{cp{11.5cm}}
            
            \ifthenelse{\equal{\license}{BY}}
            {
            \includegraphics[width=3cm]{estrutura/pre-textuais/CreativeCommonsLogos/CC_BY.png}\\{\tiny 4.0 Internacional}
            }{}
            
            \ifthenelse{\equal{\license}{BYSA}}
            {
            \includegraphics[width=3cm]{estrutura/pre-textuais/CreativeCommonsLogos/CC_BY_SA.png}\\{\tiny 4.0 Internacional}
            }{}
            
            \ifthenelse{\equal{\license}{BYND}}
            {
            \includegraphics[width=3cm]{estrutura/pre-textuais/CreativeCommonsLogos/CC_BY_ND.png}\\{\tiny 4.0 Internacional}
            }{}
                     
            \ifthenelse{\equal{\license}{BYNC}}
            {
            \includegraphics[width=3cm]{estrutura/pre-textuais/CreativeCommonsLogos/CC_BY_NC.png}\\{\tiny 4.0 Internacional}
            }{}
            
            \ifthenelse{\equal{\license}{BYNCSA}}
            {
            \includegraphics[width=3cm]{estrutura/pre-textuais/CreativeCommonsLogos/CC_BY_NC_SA.png}\\{\tiny 4.0 Internacional}
            }{}
            
            \ifthenelse{\equal{\license}{BYNCND}}
            {
            \includegraphics[width=3cm]{estrutura/pre-textuais/CreativeCommonsLogos/CC_BY_NC_ND.png}\\{\tiny 4.0 Internacional}
            }{}

            & 
            
            \ifthenelse{\equal{\license}{BY}}
            {
              \vspace{-1.8cm}{\tiny Esta licenÃ§a permite que outros distribuam, remixem, adaptem e criem a partir do trabalho licenciado, mesmo para fins comerciais, desde que atribuam o devido crÃ©dito ao autor pela criaÃ§Ã£o original. Ã‰ a licenÃ§a mais flexÃ­vel de todas as licenÃ§as disponÃ­veis. Ã‰ recomendada para maximizar a disseminaÃ§Ã£o e uso dos materiais licenciados.}
            }{}
            
            \ifthenelse{\equal{\license}{BYSA}}
            {
             \vspace{-1.8cm}{\tiny Esta licenÃ§a permite que outros remixem, adaptem e criem a partir do trabalho licenciado, mesmo para fins comerciais, desde que atribuam, ao autor, o devido crÃ©dito e que licenciem as novas criaÃ§Ãµes sob termos idÃªnticos. Esta licenÃ§a costuma ser comparada com as licenÃ§as de software livre e de cÃ³digo aberto "copyleft". Todos os trabalhos novos baseados no trabalho licenciado terÃ£o a mesma licenÃ§a, portanto quaisquer trabalhos derivados tambÃ©m permitirÃ£o o uso comercial. Esta Ã© a licenÃ§a recomendada para materiais que seriam beneficiados com a incorporaÃ§Ã£o de conteÃºdo.}
            }{}
            
            \ifthenelse{\equal{\license}{BYND}}
            {
             \vspace{-1.8cm}{\tiny Esta licenÃ§a permite a redistribuiÃ§Ã£o, comercial e nÃ£o comercial, desde que o trabalho licenciado seja distribuÃ­do inalterado e no seu todo, com crÃ©dito atribuÃ­do ao autor.}
            }{}
                     
            \ifthenelse{\equal{\license}{BYNC}}
            {
             \vspace{-1.8cm}{\tiny Esta licenÃ§a permite que outros remixem, adaptem e criem a partir do trabalho licenciado para fins nÃ£o comerciais, com crÃ©dito atribuÃ­do ao autor. Os usuÃ¡rios nÃ£o tÃªm que licenciar os trabalhos derivados sob os mesmos termos estabelecidos pelo autor do trabalho original.}

            }{}
            
            \ifthenelse{\equal{\license}{BYNCSA}}
            {
             \vspace{-1.8cm}{\tiny Esta licenÃ§a permite que outros remixem, adaptem e criem a partir do trabalho licenciado para fins nÃ£o comerciais, desde que atribuam ao autor o devido crÃ©dito e que licenciem as novas criaÃ§Ãµes sob termos idÃªnticos.}
            }{}
            
            \ifthenelse{\equal{\license}{BYNCND}}
            {
             \vspace{-1.8cm}{\tiny Esta Ã© a mais restritiva das seis licenÃ§as principais Creative Commons. Permite apenas que outros faÃ§am download dos trabalhos licenciados e os compartilhem desde que atribuam crÃ©dito ao autor, mas sem que possam alterÃ¡-los de nenhuma forma ou utilizÃ¡-los para fins comerciais.}
            }{}
           
            \end{tabular}
            \end{table}
	\end{SingleSpacing}
    \end{center}
}
\makeatother

% -----------------------------------------------------------------------------
% Folha de aprovaÃ§Ã£o
% -----------------------------------------------------------------------------
\makeatletter
\newcommand{\imprimirfolhadeaprovacao}
{%
    \begin{folhadeaprovacao}
        \vspace*{\fill}

        \begin{tabular}{p{.45\textwidth}p{.45\textwidth}}
            & \imprimirtextopadraofolhadeaprovacao \\
        \end{tabular}

        \vspace*{\fill}
    \end{folhadeaprovacao}
}
\makeatother

% -----------------------------------------------------------------------------
% DedicatÃ³ria
% -----------------------------------------------------------------------------
\renewenvironment{dedicatoria}[1][]
{%
    \ifthenelse{\equal{#1}{}}{%
        \PRIVATEbookmarkthis{\dedicatorianame}
    }{\pretextualchapter{#1}}

    \vspace*{\fill}

    \begin{flushright}
        \begin{minipage}{.5\textwidth}
            \begin{SingleSpacing}
                \setlength\parindent{0pt}
                \setlength\parskip{12pt}
}
{%
            \end{SingleSpacing}
        \end{minipage}
    \end{flushright}
}

% -----------------------------------------------------------------------------
% EpÃ­grafe
% -----------------------------------------------------------------------------
\renewenvironment{epigrafe}[1][]
{%
    \ifthenelse{\equal{#1}{}}
    {%
        \PRIVATEbookmarkthis{\epigraphname}
    }{\pretextualchapter{#1}}

    \vspace*{\fill}

    \begin{flushright}
        \begin{minipage}{.5\textwidth}
            \begin{SingleSpacing}
                \setlength\parindent{0pt}
                \setlength\parskip{12pt}
}
{%
            \end{SingleSpacing}
        \end{minipage}
    \end{flushright}
}

