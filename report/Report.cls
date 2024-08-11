% --- identification
\NeedsTeXFormat{LaTeX2e}[1995/12/01]
\ProvidesClass{standard-doc}[2001/03/03 UoBnote class]

% --- using pkgs
\RequirePackage{fancyhdr}
\RequirePackage{lastpage}
\RequirePackage[T1]{fontenc}
\RequirePackage[british]{babel}
%\RequirePackage[margin=0.5in]{geometry}
%\RequirePackage[margin=0.5in]{geometry}

% --- define options
\DeclareOption{twocolumn}{\OptionNotUsed}
\DeclareOption*{\PassOptionsToClass{\CurrentOption}{article}}
\ProcessOptions\relax

\LoadClass[a4paper,11pt]{article}

% --- load class


% color is switched on, use the color package

\def\issue#1{\gdef\@issue{#1}}
\def\@issue{\@latex@error{No \noexpand\issue given}\@ehc}
\def\shorttitle#1{\gdef\@shorttitle{#1}}
\def\@shorttitle{\@latex@error{No \noexpand\shorttitle given}\@ehc}

\renewcommand\maketitle{
  \begin{titlepage}\begin{center}
      {\includegraphics[scale=0.1]{TrentoLOGO.png}}
      \vskip2cm
      {\LARGE \bf \@title \par}%
      \vskip 3em%
      {\large
        \lineskip .75em%
        \begin{tabular}[t]{c}%
          \@author
        \end{tabular}\par}%
      \vskip 1.5em%
      {\normalsize \@date \par}%
      \vfil\null%
      {\small
        University of Trento\\
        Department of Information Engineering and Computer Science~-~DISI\\
        Via Sommarive, 9 I-38123\\
        Povo (TN), Italy\\
        % E-Mail: Username[at]star.sr.bham.ac.uk
      } \end{center}
  \end{titlepage}
}

\newcommand\fancytoc{
    \pagestyle{plain}

    \pagenumbering{roman}

    \tableofcontents{\clearpage}

    \pagestyle{fancy}

    \pagenumbering{arabic}
}

\rhead{\@title}

\lfoot{} 
\cfoot{{\footnotesize \thepage~\textit{of}~\pageref{LastPage}}} 
\rfoot{}


\message{note completed OK}