---
layout: post
title:  "Aprendizado por Reforço - Introdução "
categories: [ reinforcement_learning ]
author: adriano
permalink: "/rf/introducao.html"
image: "https://img.ibxk.com.br/2018/10/15/corpos-15114949494090.gif"
tag: [featured]
---

# Introdução a Aprendizagem por Reforço

Muitos dos conceitos aqui presentes foram retirados do livro:`Hands-On Machine Learning with Scikit-Learn & TensorFlow` - Aurélien Géron

Aprendizado por Reforço (*Reinformecent Learning*) é um dos campos mais excitantes do Aprendizado de Máquina e também um dos mais antigos. Hoje existem diversas aplicações, principalmente em jogos e controles de máquinas (robôs, carros autônomos). 

Na industria o RL ainda é pouco explorado, por dois motivos:

### 1) Maturidade
Hoje a preocupação maior é em obter, armazenar e tratar os dados. Existe o hype na industria de que as decisões orientadas a dados é importante, porém grande parte das empresas ainda não tem infraestrutura orientadas a dados. 

### 2) Relevância
Tem um pouco a ver com a maturidade. As empresas ainda não acham relevante aplicar Aprendizado por Reforço em seus produtos. Em muitas delas a área de Ciência de Dados quase não tem Inteligência, é mais um conjunto de regras e filtros do que inteligẽncia em si.

Porém, com a maturidade cada vez maior e o aumento da relevãncia da área de dados, novas possibilidades estão surgindo no mercado. Isso permite que a `Ciẽncias` (Experimentação) se torne atraente também dentro das empresas.  

## Poder do Aprendizado por Reforço
Sem dúvida, o aprendizado por reforço é uma das área mais promissoras. O que separa um simples modelo de uma tarefa complexa é apenas o tempo de treinamento.
Em 2014, uma empresa chamada DeepMind foi comprada pelo google por 500 milhões de dolares. Ela cria tecnologias baseadas em RL para todos os tipos de problemas. Na época ela ficou famosa por demonstrar que um sistema simples era capaz de aprender a jogar um jogo do Atari. Em meados de 2016, uma de suas criações AlphaGo, ganhou o campeão mundial do jogo GO. 

Hoje o google utiliza muitas das tecnologias da DeepMind para melhorar seus próprios produtos. Principalmente utilizando sistemas de recomendação, detecção de SPAM e ofertas personalizadas para cada perfil de usuário.


<img src="https://www.tecnologiae.com.br/wp-content/uploads/2017/04/aprendizagem-maquina-e1493300647272.png" />

## Aprendizado por ~Reforço~ Recompensa

No RL, um `Agente` oberva um `Ambiente` e executa determinadas `Ações` que vão lhe trazer maior `Recompensa`. Cada posição do Agente é chamada de `estado`.

> O objetivo do Agente é aprender um jeito de realizar uma tarefa que dê o maior soma total de recompensas

Pense na imagem abaixo, o objetivo do Mario é chegar até a princesa. 

O Mário não sabe o percurso até a princesa, então ele precisa achar o caminho por meio de tentativa e erro. Se ele fizer algo de errado, como por exemplo ir para uma casa onde tem um inimigo ele receberá uma **recompensa negativa**. Caso ele chegue na princesa, ganha uma **recompensa positiva**

No final, após várias tentativas, o nosso querido mário terá em maos um **MAPA** com o melhor caminho partindo de qualquer lugar (qualquer estado). 

<img src="https://miro.medium.com/max/2642/1*83qMkjkWaGP7HWfGiImyqw.png" />

## Policy Search

O termo *Policy* é dificil de traduzir. Poderia ser políticas, conjunto de regras ou estratégia. O Fato é que a *Policy* é algoritmo usado pelo agente para tomar as decisões.

Em nosso exemplo do Mário, a *Policy* seria o Mapa. Inicialmente o Mapa vem em branco e é preenchido com os **estados** e as **ações** do mario durante sua trajetória. 

*Policy Search* ou (Busca pela Política) é a busca pelas ações que o agente tem que tomar dado que ele está em um determinado estado. Chama-se `política ótima` para a política que fornece a maior soma total de recompensas. 

Existem várias formas de buscar uma política ótima ( ou preencher o melhor Mapa do Mario)

### 1) Força Bruta
Poderiamos testar para cada posição do mário qual a melhor ação. Em um cenário pequeno isso é simples de fazer. Mas pense em um robô aspirador de pó, o número de estados é muito grande ou até infinito.

### 2) Algoritmo Genético
Para ambientes onde o conjutno de estados é muito grande poderia-se utilziar algoritmos genético.  
- a) Cria-se aleatoriamente algumas politicas
- b) Testa as politicas e elimina as piores
- c) Cria-se novas politicas a apartir da combinação das melhores politicas
- d) Repete esse processo até achar uma política satisfatória.
É um boa estratégia, mas precisa de poder computacional e o tempo de execução por ser um grande problema. 

### 3) Policy Gradient
É uma técnica de otimização bastante utilizada. O gradiente utiliza derivadas para achar valores mínimos (Gradiente Descendente) ou valores máximos (Gradiente Ascendente). Quem já conhece as redes neurais já ouviu fazer do Gradiente Descendente. No algoritmo de backprograpação, o gradiente é usado para atualizar os pesos da rede neural através da minimização do erro total. No caso da RL, o objetivo é maximizar a recompensa, por isso, é utilizado o Gradiente Ascendente. 

<img src="https://matheusfacure.github.io/img/tutorial/momento.gif" />

## Ambiente (Environment)

Um dos principais desafios do Reinforcement Learning é o ambiente. Pense em um agente aprendendo a jogar Dota2, famoso jogo de estratégia entre equipes (gif abaixo). O ambiente são todas as arvores, as torres, os inimigos tudo.

<img src="https://thumbs.gfycat.com/EmbarrassedCharmingLhasaapso-size_restricted.gif" />

<a href="https://www.youtube.com/watch?v=tfb6aEUMC04"> OpenAI Vence Campeões Mundiais de Dota 2</a>

Agora, imagine que estamos programando um robozinho virtual para aprender a caminhar usando 2 pernas, como na figura abaixo. O ambiente são as descidas, subidas, obstáculos etc.

<img src='https://miro.medium.com/max/1192/1*lG_9GxvSBOvfZ1dvdUXu6A.gif' />



E se você tiver fazendo um sistema de aprendizado por reforço para dar recomendações de filmes, como acontecer no NetFlix por exemplo?

O agente seria o modelo e o ambiente seria o conjunto de caracteristicas dos filmes mais informações dos usuário por exemplo. 

O objetivo do modelo seria, para um conjunto de caracteristas de um usuário e dos filmes do catálogo, qual a melhor recomendação para este usuário.

### Pŕoximo Post Vamos para o HandsOn

<img src="https://static.imagemwhats.com.br/content/assetz/uploads/2017/10/gif-do-garoto-menino-dando-joinha.gif" />
