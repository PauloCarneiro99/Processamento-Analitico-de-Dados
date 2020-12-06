CREATE TABLE IF NOT EXISTS SCHEMA_INDI_SOCIAIS.INDICADORES_SOCIAIS
(
    CHAVE_PAIS        UUID,
    ANO               INT,
    ID_SEXO           UUID,
    IDH               FLOAT,
    ANOS_ESCOLARIDADE FLOAT,
    EXPECTATIVA_VIDA  FLOAT,

    CONSTRAINT SCHEMA_INDI_SOCIAIS_INDICADORES_SOCIAIS_PK PRIMARY KEY (CHAVE_PAIS, ANO, ID_SEXO),
    CONSTRAINT SCHEMA_INDI_SOCIAIS_INDICADORES_SOCIAIS_PAIS_FK FOREIGN KEY (CHAVE_PAIS) REFERENCES SCHEMA_INDI_SOCIAIS.LOCAL (CHAVE_PAIS),
    CONSTRAINT SCHEMA_INDI_SOCIAIS_INDICADORES_SOCIAIS_ANO_FK FOREIGN KEY (ANO) REFERENCES SCHEMA_INDI_SOCIAIS.TEMPO (ANO),
    CONSTRAINT SCHEMA_INDI_SOCIAIS_INDICADORES_SOCIAIS_ID_FK FOREIGN KEY (ID_SEXO) REFERENCES SCHEMA_INDI_SOCIAIS.SEXO (ID_SEXO)
);
