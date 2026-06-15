try:
    processar_somente_cessoes_pendentes = True

    param_lista_cessoes   = validar_param_lista("cessoes_lista_string", str)
    param_lista_operacoes = validar_param_lista("operacoes_lista_int", int)
    param_tipo_ativo      = validar_param_unico("tipo_ativo_string", str)
    param_registradora    = validar_param_unico("registradora_string", str)
    param_lista_contratos = validar_param_lista("contratos_lista_string", str)

    if param_lista_cessoes or param_lista_contratos:
        processar_somente_cessoes_pendentes = False

    if param_tipo_ativo and param_registradora:
        try: 
            fluxos_operacoes_processamento = {(param_tipo_ativo, param_registradora): FLUXOS_OPERACOES[(param_tipo_ativo, param_registradora)]}
            print(f"TIPO DE ATIVO E REGISTRADORA FILTRADOS: {param_tipo_ativo}-{param_registradora}")
        
        except Exception as e:
            raise ValueError(f'ERRO: NÃO EXISTEM FLUXOS DE OPERAÇÕES MAPEADOS PARA {param_tipo_ativo}-{param_registradora}') 
        
    else:
        fluxos_operacoes_processamento = FLUXOS_OPERACOES
        print("NÃO EXISTEM FILTROS PARA TIPO DE ATIVO E REGISTRADORA")

except Exception as e:
    detalhes_erro = traceback.format_exc()
    raise RuntimeError(f"ERRO ETAPA 1 - AO PROCESSAR PARAMETROS DO JOB PIPELINE \n{detalhes_erro}") from e

fw = SparkFramework()

r_cessoes_pendentes_de_registro = fw.run(
    f"{BASE_PATH}/conf_cessoes_pendentes_registro.json",
    params={
        "processar_somente_cessoes_pendentes": processar_somente_cessoes_pendentes,
        "lista_contratos": param_lista_contratos,
        "lista_cessoes": param_lista_cessoes,
        "lista_operacoes": param_lista_operacoes,
    }    
)
print(r_cessoes.summary())
if not r_cessoes.success:
    raise RuntimeError(f"Falha na preparação de cessões: {r_cessoes.error}")

print("✅ CONTRATOS FILTRADOS:",  param_lista_contratos) if param_lista_contratos else print("ℹ️ Sem filtros para contratos")
print("✅ CESSÕES FILTRADAS:",    param_lista_cessoes)   if param_lista_cessoes   else print("ℹ️ Sem filtros para cessões")
print("✅ OPERAÇÕES FILTRADAS:",  param_lista_operacoes) if param_lista_operacoes else print("ℹ️ Sem filtros para operações")