# Catálogo De-Para: SAP B1 → Silver → Gold

## Convenções
- **Bronze:** JSON bruto, nomenclatura original SAP (PascalCase)
- **Silver:** mesma nomenclatura SAP (rastreabilidade 1:1)
- **Gold:** snake_case em português (linguagem de negócio)

---

## Dimensões (Silver → Gold)

### BusinessPartners (OCRD)

| SAP B1 (Bronze/Silver) | Gold | Tipo | Descrição |
|------------------------|------|------|-----------|
| CardCode | cod_cliente | string | Código único do parceiro |
| CardName | nome_cliente | string | Razão social |
| CardType | tipo_cliente | string | cCustomer/cSupplier/cLead |
| GroupCode | grupo_cliente | int | Código do grupo |
| SalesPersonCode | cod_vendedor | int | Vendedor responsável |
| City | cidade | string | Cidade do cadastro |
| BillToState | uf | string | UF de cobrança |
| CreditLimit | limite_credito | double | Limite de crédito (R$) |
| FreeText | texto_analise_credito | string | Análise de crédito livre |
| CreateDate | data_cadastro | date | Data de criação |

### Items (OITM)

| SAP B1 (Bronze/Silver) | Gold | Tipo | Descrição |
|------------------------|------|------|-----------|
| ItemCode | cod_item | string | SKU do produto |
| ItemName | nome_item | string | Descrição do produto |
| ItemsGroupCode | cod_grupo_item | int | Grupo do produto |
| QuantityOnStock | qtd_estoque | double | Saldo em estoque |
| SalesUnit | unidade_venda | string | Unidade (UN, CX, etc.) |

### SalesPersons (OSLP)

| SAP B1 (Bronze/Silver) | Gold | Tipo | Descrição |
|------------------------|------|------|-----------|
| SalesEmployeeCode | cod_vendedor | int | Código do vendedor |
| SalesEmployeeName | nome_vendedor | string | Nome/código regional |
| CommissionForSalesEmployee | pct_comissao | double | % de comissão |
| Active | ativo | string | Se está ativo (tYES/tNO) |

### ItemGroups (OITB)

| SAP B1 (Bronze/Silver) | Gold | Tipo | Descrição |
|------------------------|------|------|-----------|
| Number | cod_grupo_item | int | Código do grupo |
| GroupName | nome_grupo_item | string | Nome do grupo |

---

## Fatos (Silver → Gold)

### Invoices (OINV + INV1)

| SAP B1 (Bronze/Silver) | Gold | Tipo | Descrição |
|------------------------|------|------|-----------|
| DocEntry | id_documento | int | ID interno da NF |
| DocNum | num_nota | int | Número visível da NF |
| DocDate | data_emissao | date | Data de emissão |
| CardCode | cod_cliente | string | FK → business_partners |
| CardName | nome_cliente | string | Razão social (desnormalizado) |
| DocTotal | valor_total_doc | double | Total da NF (R$) |
| VatSum | valor_impostos | double | Total de impostos |
| SalesPersonCode | cod_vendedor | int | FK → sales_persons |
| DocumentStatus | status_documento | string | bost_Open/bost_Close |
| Cancelled | cancelada | string | tYES/tNO |
| BPL_IDAssignedToInvoice | cod_filial | int | Filial emissora |
| NumberOfInstallments | qtd_parcelas | int | Nº de parcelas |
| LineNum | num_linha | int | Linha do documento |
| ItemCode | cod_item | string | FK → items |
| ItemDescription | desc_item | string | Descrição do item |
| Quantity | quantidade | double | Qtd vendida |
| UnitPrice | preco_unitario | double | Preço unitário (R$) |
| LineTotal | valor_linha | double | Total da linha (R$) |
| LineDiscountPercent | pct_desconto_linha | double | Desconto aplicado (%) |
| WarehouseCode | cod_deposito | string | Depósito de origem |
| GrossProfit | lucro_bruto | double | Margem bruta (R$) |
| GrossBuyPrice | custo_aquisicao | double | Custo unitário (R$) |
| CFOPCode | cfop | string | Código fiscal da operação |
| BaseEntry | id_doc_origem | int | DocEntry do pedido/cotação |
| BaseType | tipo_doc_origem | int | 17=Order, 23=Quotation |
| LineStatus | status_linha | string | bost_Open/bost_Close |
| FreeOfChargeBP | brinde | string | tYES se cortesia |
| ActualDeliveryDate | data_entrega_real | date | Data efetiva de entrega |

### Orders (ORDR + RDR1)
> Mesma estrutura de Invoices

### Quotations (OQUT + QUT1)
> Mesma estrutura, sem campos de frete/transporte

---

## Tabelas Gold (agregadas)

### gpcorp_gold_vendas.faturamento_mensal

| Coluna Gold | Origem | Cálculo |
|-------------|--------|---------|
| ano | DocDate.year | Extração |
| mes | DocDate.month | Extração |
| cod_vendedor | SalesPersonCode | Direto |
| cod_cliente | CardCode | Direto |
| nome_cliente | CardName | Direto |
| cod_item | ItemCode | Direto |
| desc_item | ItemDescription | Direto |
| cod_filial | BPL_IDAssignedToInvoice | Direto |
| receita_total | SUM(LineTotal) | Agregação |
| qtd_itens | SUM(Quantity) | Agregação |
| qtd_notas | COUNT(DISTINCT DocEntry) | Agregação |
| preco_medio | AVG(UnitPrice) | Agregação |
| pct_desconto_medio | AVG(LineDiscountPercent) | Agregação |
| lucro_bruto | SUM(GrossProfit) | Agregação |

### gpcorp_gold_vendas.ranking_vendedores

| Coluna Gold | Origem | Cálculo |
|-------------|--------|---------|
| ranking | — | DENSE_RANK() por receita DESC |
| receita_faturada | SUM(LineTotal) | Agregação |
| lucro_bruto | SUM(GrossProfit) | Agregação |
| qtd_notas | COUNT(DISTINCT DocEntry) | Agregação |
| qtd_clientes | COUNT(DISTINCT CardCode) | Agregação |
| mix_produtos | COUNT(DISTINCT ItemCode) | Agregação |
| ticket_medio_linha | AVG(LineTotal) | Agregação |
| pct_margem_bruta | SUM(GrossProfit)/SUM(LineTotal)*100 | Cálculo |

### gpcorp_gold_cotacoes.taxa_conversao

| Coluna Gold | Origem | Cálculo |
|-------------|--------|---------|
| total_cotacoes | COUNT(DISTINCT Quotations.DocEntry) | Agregação |
| valor_cotacoes | SUM(Quotations.LineTotal) | Agregação |
| total_pedidos | COUNT(DISTINCT Orders.DocEntry) | Agregação |
| taxa_conversao | total_pedidos / total_cotacoes | Cálculo |

---

## Valores especiais do SAP B1

| Valor | Significado |
|-------|-------------|
| `tYES` / `tNO` | Booleano SAP (Yes/No) |
| `bost_Open` | Documento aberto |
| `bost_Close` | Documento fechado/faturado |
| `-1` | Sem vínculo (sentinela numérico) |
| `dDocument_Items` | Tipo de documento: itens |
| `cCustomer` | Parceiro tipo Cliente |
| `cSupplier` | Parceiro tipo Fornecedor |

---

## Campos de controle (prefixo _)

| Campo | Camada | Descrição |
|-------|--------|-----------|
| `_silver_loaded_at` | Silver | Timestamp UTC da carga |
| `_silver_load_date` | Silver | Data da carga |
| `_sk` | Silver (dimensões) | Surrogate key (SHA-256 da PK) |
| `_row_hash` | Silver (SCD2) | Hash para detecção de mudanças |
| `_valid_from` | Silver (SCD2) | Início da validade da versão |
| `_valid_to` | Silver (SCD2) | Fim da validade (NULL = corrente) |
| `_is_current` | Silver (SCD2) | Versão ativa (true/false) |
| `_gold_loaded_at` | Gold | Timestamp da geração Gold |
