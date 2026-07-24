<h1 align="center">
  ⚽ TCC: Modelo Computacional para Recomendação Tática
</h1>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" />
  <img src="https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white" />
  <img src="https://img.shields.io/badge/Scikit_Learn-F7931E?style=for-the-badge&logo=scikit-learn&logoColor=white" />
  <img src="https://img.shields.io/badge/MariaDB-003545?style=for-the-badge&logo=mariadb&logoColor=white" />
  <img src="https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black" />
</p>

> **Sistema de recomendação tática para futebol baseado em arquétipos de jogadores e análise de dados do Brasileirão.**

Este repositório contém o código-fonte e a modelagem do meu Trabalho de Conclusão de Curso (TCC) em Ciência da Computação pela FURB (2026/1). O projeto visa auxiliar decisões táticas no futebol por meio de inteligência de dados, transcendendo a visão clássica de "posições nominais" para uma abordagem baseada em "funções" (arquétipos).

---

## 🚀 O Projeto

O futebol moderno é ditado pela organização coletiva e dinâmica de espaços. O rótulo genérico de um "Camisa 10" ou "Volante" não explica mais o jogo atual. 

Para resolver essa insuficiência analítica, este modelo extrai estatísticas avançadas, classifica os atletas em perfis funcionais exatos (baseados na taxonomia do EA Sports FC) e utiliza otimização combinatória para escalar o time titular de forma inteligente, maximizando as chances de sucesso com base em matchups táticos.

## ⚙️ Arquitetura e Pipeline de Dados

O fluxo de processamento passa pelas seguintes etapas principais:

1. **Extração e Pré-Processamento:**
   - Coleta de dados brutos da **Série A do Brasileirão** através da plataforma SofaScore.
   - Tratamento de métricas utilizando cálculo **P90 (Por 90 Minutos)** para garantir equidade na minutagem.
   - Normalização de escalas através de **Min-Max Scaling**.

2. **Classificação de Arquétipos (Motor de Similaridade):**
   - Construção de vetores estatísticos baseados em jogadores de referência da elite europeia (denominados "Deuses" do modelo, como Virgil van Dijk e Gabriel Magalhães).
   - Cálculo de **Similaridade de Cosseno Ponderada** para medir o ângulo e a proximidade do vetor do jogador analisado com o arquétipo tático ideal, descartando similaridades inferiores a 10%.

3. **Otimização Global de Escalação (Matchup):**
   - Utilização de **Programação Linear Inteira (ILP)** para definir a combinação ideal dos 11 jogadores, respeitando rigidamente os "slots" da formação tática requerida (ex: 4-3-3).
   - Implementação do **Algoritmo Húngaro** como *fallback* estratégico caso a equipe atue em formações não reconhecidas ou não padronizadas.

4. **Visualização:**
   - Consolidação dos relatórios em um banco de dados relacional **MariaDB**.
   - Integração das análises via **Power BI**, englobando interfaces como: Modo Simulador, Modo Treinador, Central do Elenco, Painel de Estatísticas e Visão Geral.

## 👨‍💻 Autor

**Martin Lange de Assis**  
*Graduando em Ciência da Computação - Universidade Regional de Blumenau (FURB)*  
*Orientador: Prof. Aurélio Faustino Hoppe*

---
*Este projeto foi desenvolvido com foco acadêmico para unir a paixão pela análise esportiva às tecnologias de Ciência de Dados e Pesquisa Operacional.*
