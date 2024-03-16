Projeto de Modelagem de Regressão com Dados do TMDB utilizando pipelines do GCP

Descrição do Projeto:
Este projeto visa desenvolver um pipeline de análise de dados e modelagem de regressão utilizando dados obtidos da API do The Movie Database (TMDB). O objetivo é prever a avaliação média (vote_average) de filmes com base em diversas características, como a popularidade, o número de votos (vote_count) e o ano de lançamento.

O pipeline de análise de dados inclui as seguintes etapas:
1.	Extração de Dados da API do TMDB: Utilizando a biblioteca google.cloud.bigquery, os dados são extraídos diretamente da API do TMDB, incluindo informações relevantes sobre os filmes.
2.	Pré-processamento dos Dados: Os dados são pré-processados para tratamento de valores nulos, conversão de tipos de dados e criação de novas features, como o ano de lançamento.
3.	Modelagem com SVR (Support Vector Regression): Um modelo de regressão utilizando SVR é treinado com os dados pré-processados para prever a avaliação média dos filmes.
4.	Avaliação do Modelo: O desempenho do modelo é avaliado utilizando a métrica de erro quadrático médio (MSE) tanto nos dados de treino quanto nos dados de teste.
5.	Exportação dos Resultados: Os resultados finais do modelo, incluindo as previsões e as avaliações de desempenho, são exportados para um arquivo CSV e armazenados no Google Cloud Storage.
6.	Apresentação da exportação TMDB em uma painel construído no Looker.
   
Tecnologias Utilizadas:
•	Python
•	Google Cloud Platform (BigQuery, Cloud Storage, Composer)
•	Airflow
•	Scikit-learn
•	Pandas
•	Jupyter Notebook
• Looker

Resultados:
Os resultados do modelo treinado estão disponíveis no arquivo ML_results.csve. Além disso, os detalhes do modelo, incluindo os melhores parâmetros e métricas de desempenho, podem ser encontrados na saída do script Python.
Este projeto foi desenvolvido como parte de um estudo sobre modelagem de regressão com dados reais e pode servir como exemplo de aplicação de técnicas de aprendizado de máquina em dados do mundo real, utilizando a arquitetura de pipelines da Google Cloud.

