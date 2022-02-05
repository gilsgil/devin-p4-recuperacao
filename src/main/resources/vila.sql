CREATE TABLE habitantes
(
    id             SERIAL PRIMARY KEY,
    nome           varchar(30)    NOT NULL,
    sobreNome      varchar(30)    NOT NULL,
    dataNascimento DATE           NOT NULL,
    renda          decimal(10, 2) NOT NULL,
    cpf            varchar(14)    NOT NULL UNIQUE
);

INSERT INTO habitantes
VALUES (DEFAULT, 'user', 'vila', '1999-05-25', '2000.00', '123.456.789-01');

INSERT INTO habitantes
VALUES (DEFAULT, 'admin', 'vila', '1988-03-13', '5000.00', '987.654.321-09');

CREATE TABLE "user"
(
    id           SERIAL PRIMARY KEY,
    email        varchar(100) NOT NULL UNIQUE,
    password     varchar(255) NOT NULL,
    roles        varchar[3]   NOT NULL DEFAULT '{USER}',
    habitante_id bigint,
    CONSTRAINT user_table_habitante_fk FOREIGN KEY (habitante_id) REFERENCES habitantes (id) ON DELETE CASCADE
);

INSERT INTO "user" (email, password, roles, habitante_id)
VALUES ('user@vila.com.br', 'user', '{USER}', 1);

INSERT INTO "user" (email, password, roles, habitante_id)
VALUES ('admin@vila.com.br', 'admin', '{USER, ADMIN}', 2);