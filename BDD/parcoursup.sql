DROP TABLE IF EXISTS import;

DROP TABLE IF EXISTS inscrits;
DROP TABLE IF EXISTS promotion cascade;
DROP TABLE IF EXISTS formation cascade;
DROP TABLE IF EXISTS etablissement cascade;
DROP TABLE IF EXISTS communes cascade;

DROP SEQUENCE IF EXISTS communes_cno_seq;
DROP SEQUENCE IF EXISTS etablissement_seq;


\! curl -o fr-esr-parcoursup.csv -L https://data.enseignementsup-recherche.gouv.fr/api/explore/v2.1/catalog/datasets/fr-esr-parcoursup/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B

SELECT pg_sleep(20);

\echo ``nombre de lignes du fichier initial avec header inclut''
\! wc -l fr-esr-parcoursup.csv


CREATE temp TABLE import (
    n1 int default null,n2 text default null,n3 char(8) default null,n4 text default null,n5 char(3) default null,n6 text default null,n7 text default null,n8 text default null,n9 text default null,
    n10 text default null,n11 text default null,n12 text default null,n13 text default null,n14 text default null,n15 text default null,n16 text default null,n17 text default null,
    n18 int default null,n19 int default null,n20 int default null,n21 int default null,n22 text default null,n23 int default null,n24 int default null,n25 int default null,
    n26 int default null,n27 int default null,n28 int default null,n29 int default null,n30 int default null,n31 int default null,n32 int default null,n33 int default null,
    n34 int default null,n35 int default null,n36 int default null,n37 text default null,n38 text default null,n39 int default null,n40 int default null,n41 int default null,
    n42 int default null,n43 int default null,n44 int default null,n45 int default null,n46 int default null,n47 int default null,n48 int default null,n49 int default null,
    n50 int default null,n51 real default null,n52 real default null,n53 real default null,n54 text default null,n55 int default null,n56 int default null,n57 int default null,
    n58 int default null,n59 int default null,n60 int default null,n61 int default null,n62 int default null,n63 int default null,n64 int default null,n65 int default null,
    n66 real default null,n67 real default null,n68 int default null,n69 int default null,n70 text default null,n71 text default null,n72 int default null,n73 int default null,
    n74 real default null,n75 real default null,n76 real default null,n77 real default null,n78 real default null,n79 real default null,n80 real default null,n81 real default null,
    n82 real default null,n83 real default null,n84 real default null,n85 real default null,n86 real default null,n87 real default null,n88 real default null,n89 real default null,
    n90 real default null,n91 real default null,n92 real default null,n93 real default null,n94 real default null,n95 real default null,n96 real default null,n97 real default null,
    n98 real default null,n99 real default null,n100 real default null,n101 real default null,n102 text default null,n103 text default null,n104 text default null,n105 text default null,
    n106 text default null,n107 text default null,n108 text default null,n109 text default null,n110 text default null,n111 text default null,n112 text default null,n113 text default null,
    n114 real default null,n115 real default null,n116 real default null,n117 text default null,n118 text );

\copy import from fr-esr-parcoursup.csv WITH(delimiter ';', HEADER)

\! rm fr-esr-parcoursup.csv

CREATE TABLE communes (nomCom, région, département, codeDépartement)
    AS SELECT DISTINCT n9, n7, n6, n5 FROM import
    WHERE n9 <> '';
ALTER TABLE communes ADD cno SERIAL;

CREATE TABLE etablissement (statusEta ,nomEta, academie, cno)
    AS SELECT DISTINCT n2, n4, n8, cno FROM import, communes
    WHERE communes.nomCom = n9;
ALTER TABLE etablissement ADD eno SERIAL;

CREATE TABLE formation (codeFormation, 
                        UAI, 
                        filièreDeFormation, 
                        Selectivité, 
                        filièreDeFormationTrèsAgrégée,
                        coordonnees, 
                        annee, 
                        capacité, 

                        eff_total, 
                        eff_candidates,

                        eff_total_phase_principale, 
                        eff_total_internat,
                        eff_total_classe_principale, 
                        eff_total_classe_complementaire,
                        eff_total_classe_internat, 
                        eff_total_recu_proposition,

                        eff_admises, 
                        eff_admis_principale, 
                        eff_admis_complementaire,
                        eff_admis_internat, 
                        eff_admis_boursiers_neobacheliers,

                        eff_admis_recu_proposition_ouverture,
                        eff_admis_recu_proposition_avant_fin, 

                        eff_admis_sans_mention,
                        eff_admis_mentionAB,
                        eff_admis_mentionB,
                        eff_admis_mentionTB,
                        eff_admis_mentionTB_felicitation)


    AS SELECT DISTINCT n110 , 
                       n3, 
                       n10, 
                       n11, 
                       n12, 
                       n17, 
                       n1, 
                       n18,

                       n19, 
                       n20, 

                       n21, 
                       n22, 
                       n35,
                       n36, 
                       n37, 
                       n46, 

                       n48, 
                       n49, 
                       n50, 
                       n54, 
                       n55, 

                       n51, 
                       n53, 

                       n62, 
                       n63, 
                       n64,
                       n65, 
                       n66,
                       eno FROM import, etablissement
                       WHERE etablissement.nomEta = n4
                       AND etablissement.academie = n8;
ALTER TABLE formation ADD fno SERIAL;
--ALTER TABLE formation ADD eno INT;

ALTER TABLE etablissement ADD PRIMARY KEY(eno);
ALTER TABLE formation ADD PRIMARY KEY(fno);
ALTER TABLE communes ADD PRIMARY KEY(cno);

ALTER TABLE etablissement ADD FOREIGN KEY(cno) REFERENCES communes(cno) on update cascade;
ALTER TABLE formation ADD FOREIGN KEY(eno) REFERENCES etablissement(eno) on update cascade;


CREATE TABLE inscrits (
    fno int,
    typeCandidat text check(typeCandidat = 'généraux' OR typeCandidat = 'technologiques' 
        OR typeCandidat = 'professionnels' OR typeCandidat = 'autres'),
    eff_phase_principale int,
    eff_phase_principale_boursiers int,
    eff_phase_complementaire int,
    eff_classés int,
    eff_classés_boursiers int, 
    eff_admis int,
    eff_admis_mention int,
    constraint pk_inscrits primary key (fno, typeCandidat),
    constraint fk_formation foreign key (fno)
    references formation(fno)
    on update cascade
);

insert into inscrits (fno, 
                      typeCandidat, 
                      eff_phase_principale,
                      eff_phase_principale_boursiers,
                      eff_phase_complementaire, 
                      eff_classés, 
                      eff_classés_boursiers, 
                      eff_admis, 
                      eff_admis_mention)
select fno, 'généraux', n23, n24, n31, n39, n40, n57, n67
from formation, import
where codeFormation = n110;

INSERT INTO inscrits(fno, 
                     typeCandidat, 
                     eff_phase_principale, 
                     eff_phase_principale_boursiers,
                     eff_phase_complementaire, 
                     eff_classés, 
                     eff_classés_boursiers, 
                     eff_admis, 
                     eff_admis_mention)
select fno, 'technologiques', n25, n26, n32, n41, n42, n58, n68
from formation, import
where codeFormation = n110;

INSERT INTO inscrits(fno, 
                     typeCandidat, 
                     eff_phase_principale, 
                     eff_phase_principale_boursiers,
                     eff_phase_complementaire, 
                     eff_classés, 
                     eff_classés_boursiers, 
                     eff_admis, 
                     eff_admis_mention)
select fno, 'professionnels', n27, n28, n33, n43, n44, n59, n69
from formation, import
where codeFormation = n110;

INSERT INTO inscrits(fno, 
                     typeCandidat, 
                     eff_phase_principale, 
                     eff_phase_complementaire, 
                     eff_classés, 
                     eff_admis)
select fno, 'autres', n29, n34, n45, n60
from formation, import
where codeFormation = n110;


--ALTER TABLE inscrits ADD PRIMARY KEY(typeCandidat);

--ALTER TABLE inscrits ADD FOREIGN KEY(fno) REFERENCES formation(fno) on update cascade;

