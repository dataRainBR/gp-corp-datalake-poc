# Dicionário de Dados — Camada Gold

## Domínio: Vendas (`gpcorp_gold_vendas`)

### vendas_detalhada
**Grão:** 1 linha por item de NF (drill-down)  
**Partição:** ano/mes/dia  

| Campo | Tipo | Descrição | Uso no Dashboard |
|-------|------|-----------|-----------------|
| num_doc | int | DocEntry (PK interna SAP) | Identificador |
| num_nota | int | Número da NF | Filtro/Detalhe |
| num_linha | int | Linha do documento | PK composta |
| data_nota | date | Data de emissão | Eixo temporal |
| ano | int | Ano (partição) | Filtro |
| mes | int | Mês (partição) | Filtro |
| dia | int | Dia (partição) | Filtro |
| cod_cliente | string | CardCode | FK cliente |
| nome_cliente | string | CardName (desnormalizado da NF) | Label |
| cod_vendedor | int | SalesPersonCode | FK vendedor |
| nome_vendedor | string | SalesEmployeeName (join dimensão) | Label / Filtro |
| cod_item | string | ItemCode | FK produto |
| desc_item | string | ItemDescription | Label |
| quantidade | double | Quantidade vendida | Métrica |
| preco_unitario | double | Preço unitário (sem impostos) | Métrica |
| valor_linha | double | LineTotal (líquido) | Métrica principal |
| valor_total | double | GrossTotal (com impostos) | Métrica receita bruta |
| lucro_bruto | double | GrossProfit | Métrica margem |
| pct_desconto | double | % desconto na linha | Métrica |
| cod_filial | string | WarehouseCode | Filtro filial |
| nome_filial | string | BPLName (filial emissora) | Label / Filtro |
| cfop | string | Código fiscal (CFOP) | Classificação fiscal |
| status_linha | string | Status (open/closed) | Filtro |
| cod_filial_empresa | int | BranchId | FK filial |
| num_parcelas | int | Número de parcelas | Detalhe |
| despesas_adicionais | double | DocumentAdditionalExpenses (frete/despesas no cabeçalho da NF) | Reconciliação fiscal |
| cod_grupo_item | int | Grupo do item | Agrupamento |
| nome_grupo_item | string | Nome do grupo | Label |
| marca | string | U_SX_Marca (campo customizado SAP, join via items) | Filtro/Agrupamento |
| _gold_loaded_at | timestamp | Data/hora carga Gold | Auditoria |

---

### faturamento_mensal
**Grão:** ano/mês × vendedor × cliente × item × filial  
**Partição:** ano/mes  

| Campo | Tipo | Descrição | Uso no Dashboard |
|-------|------|-----------|-----------------|
| ano | int | Ano | Eixo temporal |
| mes | int | Mês | Eixo temporal |
| cod_vendedor | int | SalesPersonCode | Filtro/Agrupamento |
| cod_cliente | string | CardCode | Filtro |
| nome_cliente | string | CardName | Label |
| cod_item | string | ItemCode | Agrupamento |
| desc_item | string | Descrição item | Label |
| cod_filial | string | WarehouseCode | Filtro |
| receita_total | double | SUM(LineTotal) | **KPI principal** |
| qtd_itens | double | SUM(Quantity) | Volume |
| qtd_notas | int | COUNT DISTINCT(DocEntry) | Frequência |
| preco_medio | double | AVG(UnitPrice) | Ticket |
| pct_desconto_medio | double | AVG(DiscountPercent) | Política comercial |
| lucro_bruto | double | SUM(GrossProfit) | Margem |
| primeira_venda_periodo | string | MIN(DocDate) | Contexto |
| ultima_venda_periodo | string | MAX(DocDate) | Contexto |
| _gold_loaded_at | timestamp | Carga | Auditoria |

---

### ranking_vendedores
**Grão:** ano/mês × vendedor  
**Partição:** ano/mes  

| Campo | Tipo | Descrição | Uso no Dashboard |
|-------|------|-----------|-----------------|
| ano | int | Ano | Eixo |
| mes | int | Mês | Eixo |
| cod_vendedor | int | Código vendedor | PK |
| nome_vendedor | string | Nome (join dimensão) | Label |
| receita_faturada | double | SUM(LineTotal) | **Ranking** |
| lucro_bruto | double | SUM(GrossProfit) | Margem |
| qtd_notas | int | COUNT DISTINCT(DocEntry) | Volume |
| qtd_clientes | int | COUNT DISTINCT(CardCode) | Carteira |
| mix_produtos | int | COUNT DISTINCT(ItemCode) | Diversidade |
| qtd_itens_vendidos | double | SUM(Quantity) | Volume |
| ticket_medio_linha | double | AVG(LineTotal) | Ticket |
| pct_margem_bruta | double | GrossProfit/LineTotal × 100 | % Margem |
| ranking | int | DENSE_RANK por receita desc | **Posição** |
| _gold_loaded_at | timestamp | Carga | Auditoria |

---

### vendas_por_produto
**Grão:** ano/mês × item  
**Partição:** ano/mes  

| Campo | Tipo | Descrição | Uso no Dashboard |
|-------|------|-----------|-----------------|
| ano | int | Ano | Eixo |
| mes | int | Mês | Eixo |
| cod_item | string | ItemCode | PK |
| nome_item | string | ItemName (join dimensão) | Label |
| cod_grupo_item | int | ItemsGroupCode | Agrupamento |
| nome_grupo_item | string | GroupName | Label |
| receita | double | SUM(LineTotal) | Métrica |
| quantidade | double | SUM(Quantity) | Volume |
| lucro_bruto | double | SUM(GrossProfit) | Margem |
| qtd_notas | int | Notas com esse item | Frequência |
| qtd_clientes | int | Clientes que compraram | Penetração |
| qtd_vendedores | int | Vendedores que venderam | Cobertura |
| preco_medio | double | AVG(UnitPrice) | Pricing |
| _gold_loaded_at | timestamp | Carga | Auditoria |

---

## Domínio: Cotações (`gpcorp_gold_cotacoes`)

### taxa_conversao
**Grão:** ano/mês × vendedor × item  

| Campo | Tipo | Descrição | Uso no Dashboard |
|-------|------|-----------|-----------------|
| ano | int | Ano | Eixo |
| mes | int | Mês | Eixo |
| cod_vendedor | int | Código vendedor | Filtro |
| nome_vendedor | string | Nome vendedor | Label |
| cod_item | string | ItemCode | Agrupamento |
| desc_item | string | Descrição | Label |
| total_cotacoes | int | COUNT DISTINCT(DocEntry) cotações | Volume |
| valor_cotacoes | double | SUM(LineTotal) cotações | Valor pipeline |
| qtd_cotada | double | Quantidade cotada | Volume |
| total_pedidos | int | Pedidos gerados | Conversão |
| valor_pedidos | double | SUM(LineTotal) pedidos | Valor convertido |
| qtd_pedida | double | Quantidade convertida | Volume |
| taxa_conversao | double | pedidos / cotações (0 a 1) | **KPI principal** |
| _gold_loaded_at | timestamp | Carga | Auditoria |

---

### features_predicao_conversao
**Grão:** cliente × vendedor  

| Campo | Tipo | Descrição | Uso |
|-------|------|-----------|-----|
| cod_cliente | string | CardCode | PK |
| cod_vendedor | int | SalesPersonCode | PK |
| total_cotacoes | int | Cotações do par | Feature |
| valor_total_cotacoes | double | Valor total cotado | Feature |
| valor_medio_linha_cotacao | double | Ticket médio cotação | Feature |
| diversidade_itens_cotados | int | Mix cotado | Feature |
| data_ultima_cotacao | date | Recência cotação | Feature |
| data_primeira_cotacao | date | Antiguidade | Feature |
| total_pedidos | int | Pedidos gerados | Feature |
| valor_total_pedidos | double | Valor convertido | Feature |
| valor_medio_linha_pedido | double | Ticket pedido | Feature |
| diversidade_itens_pedidos | int | Mix pedido | Feature |
| data_ultimo_pedido | date | Recência pedido | Feature |
| data_primeiro_pedido | date | Primeiro pedido do par cliente x vendedor | Feature |
| taxa_conversao | double | Win-rate (0 a 1) | **Target** |
| dias_ciclo_venda | int | Dias cotação→pedido | Feature |
| recencia_ultima_cotacao_dias | int | Dias desde última cotação | Feature |
| recencia_ultimo_pedido_dias | int | Dias desde último pedido | Feature |
| tipo_cliente | string | cCustomer/cSupplier | Segmentação |
| grupo_cliente | int | GroupCode | Segmentação |
| limite_credito | double | CreditLimit | Feature |
| nome_vendedor | string | Nome | Label |
| pct_comissao_vendedor | double | % comissão | Feature |
| _gold_loaded_at | timestamp | Carga | Auditoria |

---

## Domínio: Cadastros (`gpcorp_gold_cadastros`)

### perfil_cliente
**Grão:** 1 por cliente  

| Campo | Tipo | Descrição | Uso no Dashboard |
|-------|------|-----------|-----------------|
| cod_cliente | string | CardCode | PK |
| data_ultima_compra | date | Recência | RFV |
| data_primeira_compra | date | Antiguidade | Contexto |
| recencia_dias | int | Dias sem comprar | **Segmentação** |
| total_notas | int | Frequência absoluta | RFV |
| meses_ativos | int | Meses com compra | Regularidade |
| total_linhas | int | Total linhas NF | Volume |
| valor_total_compras | double | Valor lifetime | **RFV - Valor** |
| ticket_medio_linha | double | Ticket médio | Perfil |
| lucro_bruto_total | double | Margem gerada | Rentabilidade |
| diversidade_itens | int | Mix comprado | Perfil |
| vendedores_atenderam | int | Qtd vendedores | Cobertura |
| pct_rank_receita | double | Percentil (0-1) | ABC |
| classificacao_abc | string | A/B/C | **Segmentação** |
| pct_margem_media | double | % margem | Rentabilidade |
| meses_desde_primeira_compra | int | Tempo de relacionamento | Contexto |
| frequencia_mensal | double | Notas/mês | Regularidade |
| segmento_atividade | string | ativo/em_risco/inativo_recente/inativo_cronico | **Segmentação** |
| tipo_cliente | string | cCustomer/cSupplier | Filtro |
| grupo_cliente | int | GroupCode | Filtro |
| limite_credito | double | Limite configurado | Crédito |
| saldo_conta_corrente | double | Saldo atual | Crédito |
| cidade | string | Cidade | Geo |
| uf | string | Estado | Geo / Filtro |
| _gold_loaded_at | timestamp | Carga | Auditoria |

---

### catalogo_produtos_ativo
**Grão:** 1 por produto  

| Campo | Tipo | Descrição | Uso no Dashboard |
|-------|------|-----------|-----------------|
| cod_item | string | ItemCode | PK |
| nome_item | string | ItemName | Label |
| nome_estrangeiro | string | ForeignName | Ref |
| cod_grupo | int | Grupo | Agrupamento |
| nome_grupo | string | Nome grupo | Label |
| estoque_atual | double | QuantityOnStock | Disponibilidade |
| valid | string | Valid (tYES/tNO — item ativo no cadastro SAP) | Filtro cadastro |
| unidade_venda | string | SalesUnit | Ref |
| unidade_compra | string | PurchaseUnit | Ref |
| marca | string | U_SX_Marca (campo customizado SAP) | Filtro/Agrupamento |
| receita_total | double | Receita histórica | **Ranking** |
| qtd_total_vendida | double | Volume histórico | Ranking |
| preco_medio_praticado | double | Preço real de venda | Pricing |
| preco_minimo | double | Menor preço praticado | Range |
| preco_maximo | double | Maior preço praticado | Range |
| qtd_clientes_compraram | int | Penetração | Cobertura |
| qtd_notas | int | Frequência | Volume |
| data_ultima_venda | date | Última venda | Freshness |
| data_primeira_venda | date | Primeira venda | Histórico |
| dias_sem_venda | int | Inatividade | **Status** |
| receita_30d | double | Receita últimos 30d | Tendência |
| qtd_30d | double | Volume últimos 30d | Tendência |
| receita_90d | double | Receita últimos 90d | Tendência |
| qtd_90d | double | Volume últimos 90d | Tendência |
| vendeu_30d | boolean | Flag ativo 30d | Filtro |
| vendeu_90d | boolean | Flag ativo 90d | Filtro |
| status_produto | string | ativo/lento/encalhado/obsoleto/inativo_cadastro | **Segmentação** |
| classificacao_abc_produto | string | A/B/C/sem_venda | **Pareto** |
| _gold_loaded_at | timestamp | Carga | Auditoria |

---

### cobertura_vendedor
**Grão:** 1 por vendedor  

| Campo | Tipo | Descrição | Uso no Dashboard |
|-------|------|-----------|-----------------|
| cod_vendedor | int | SalesEmployeeCode | PK |
| nome_vendedor | string | Nome | Label |
| pct_comissao | double | % comissão | Contexto |
| vendedor_ativo | string | tYES/tNO | Filtro |
| receita_total | double | Receita lifetime | **Ranking** |
| lucro_bruto_total | double | Lucro gerado | Rentabilidade |
| total_notas | int | Volume absoluto | Frequência |
| total_clientes | int | Carteira total | Cobertura |
| mix_produtos | int | Diversidade | Cobertura |
| ticket_medio_linha | double | Ticket | Perfil |
| data_ultima_venda | date | Recência | Status |
| data_primeira_venda | date | Antiguidade | Contexto |
| meses_ativos | int | Meses vendendo | Regularidade |
| clientes_ativos_90d | int | Carteira ativa (90d) | **KPI** |
| clientes_inativos_90d | int | Carteira dormindo | **Alerta** |
| pct_carteira_ativa | double | % ativos/total | **KPI** |
| pct_margem | double | % margem média | Rentabilidade |
| receita_por_cliente | double | Receita/cliente | Eficiência |
| _gold_loaded_at | timestamp | Carga | Auditoria |

---

### analise_credito
**Grão:** 1 por cliente (NLP do FreeText)  

| Campo | Tipo | Descrição | Uso |
|-------|------|-----------|-----|
| cod_cliente | string | CardCode | PK |
| nome_cliente | string | CardName | Label |
| score_credito | int | Score (0-1000) | Risco |
| score_is_default | boolean | Score = Default | Alerta |
| prob_inadimplencia_pct | double | % probabilidade | Risco |
| risco_credito | string | Classificação textual | Segmentação |
| pratica_mercado | string | Prazo/vista/mista | Política |
| apenas_vista | boolean | Só vende à vista | Restrição |
| limite_sugerido_pj | double | Limite PJ sugerido | Crédito |
| limite_aprovado | double | Limite final | Crédito |
| valor_em_aberto | double | Em aberto | Exposição |
| tempo_cnpj_anos | int | Idade empresa | Maturidade |
| capital_social | double | Capital declarado | Porte |
| faturamento_presumido | double | Faturamento est. | Porte |
| cnd_status | string | negativa/positiva | Regularidade |
| tem_restricoes | boolean | Possui restrições | Alerta |
| qtd_protestos | int | Número protestos | Risco |
| decisao_final | string | aprovado_prazo/apenas_vista/limite_zerado | **Decisão** |
| ultimo_analista | string | Quem analisou | Auditoria |
| ultima_analise_data | string | Quando | Freshness |
| data_fundacao | string | Data de fundação da empresa (extraída do FreeText) | Contexto |
| valor_protestos | double | Valor total em protestos | Risco |
| qtd_socios | int | Número de sócios da empresa | Contexto |
| recebimentos_sap | double | Recebimentos já registrados no SAP | Contexto |
| qtd_titulos | int | Total de títulos em aberto | Exposição |
| qtd_titulos_atrasados | int | Títulos em atraso | **Alerta** |
| _gold_loaded_at | timestamp | Carga | Auditoria |

---

## Domínio: Estoque (`gpcorp_gold_estoque`)

### movimentacao_estoque
**Grão:** ano/mês × item × depósito  
**Partição:** ano/mes  

| Campo | Tipo | Descrição | Uso |
|-------|------|-----------|-----|
| ano | int | Ano | Eixo |
| mes | int | Mês | Eixo |
| cod_item | string | ItemCode | Agrupamento |
| nome_item | string | ItemName | Label |
| cod_grupo | int | Grupo | Agrupamento |
| nome_grupo | string | Nome grupo | Label |
| cod_deposito | string | WarehouseCode | Filtro |
| qtd_entrada | double | Volume entrada (Quantity > 0) | Métrica |
| qtd_saida | double | Volume saída (Quantity < 0, valor absoluto) | Métrica |
| valor_entrada | double | Custo das entradas (LineTotal quando Quantity > 0) | Valor |
| valor_saida | double | Custo das saídas (LineTotal quando Quantity < 0, absoluto) | Valor |
| saldo_qtd | double | SUM(Quantity) líquido do período | **Tendência** |
| saldo_valor | double | SUM(LineTotal) líquido do período | **Tendência** |
| qtd_documentos | int | COUNT DISTINCT(DocEntry) | Frequência |
| qtd_linhas | int | Total de linhas de movimentação | Volume |
| preco_medio_unitario | double | AVG(UnitPrice) | Pricing |
| primeira_movimentacao | date | MIN(DocDate) do período | Contexto |
| ultima_movimentacao | date | MAX(DocDate) do período | Contexto |
| _gold_loaded_at | timestamp | Carga | Auditoria |
