source("reportingAPI.R")
source("clavesCSOD_pro.R")
library(dplyr)
library(tidyr)
library(lubridate)
library(tictoc)

#Llamamos a WS para obtener los IDs internos de todas las vacantes
tic("llamada a reporting API")
sesion <- obtenerTokenSesion(apiSecret, apiKey)
vw_rpt_recruiting <-
    leerOdata("vw_rpt_recruiting",
              sesion$sessionToken,
              sesion$sessionSecretKey,
              "")
toc()
tic("cálculo de vacantes para la landing page")
mapeo <- vw_rpt_recruiting %>% select(ats_req_ref, ats_req_id)

#Leemos los ficheros base para la macro
parte_B <- data.table()
parte_B <-
    fread("data/Landing_Page_Input_B_17_41_12.txt", encoding = "UTF-8")
columnas_B <- names(parte_B)
parte_A <-
    read.csv("data/Landing_Page_Input_A_20_07_49.csv", sep =
                 ";")

columnas_A <- c(
    "ID DE USUARIO",
    "Nombre de usuario",
    "Apellido de usuario",
    "Unidad Organizativa ID",
    "Mis Preferencias para ofertas de empleo (Opción 1)",
    "Mis Preferencias para ofertas de empleo (Opción 2)",
    "Mis Preferencias para ofertas de empleo (Opción 3)",
    "Mis Preferencias para ofertas de empleo (Opción 4)"
)
colnames(parte_A) <- columnas_A

#Convertimos los ids externos de vacantes a ids internos
parte_B <-
    merge(parte_B,
          mapeo,
          by.x = "Id. de oferta de empleo",
          by.y = "ats_req_ref",
          all.x = TRUE)
parte_B$`Id. de oferta de empleo` <- parte_B$ats_req_id
parte_B$ats_req_id <- NULL
parte_B <- parte_B[, columnas_B, with = FALSE]


#Preparamos el DF de salida
columnas_out <- c('ID de Usuario',
                  'Nombre de pila',
                  'Apellidos',
                  'Unidad Organizativa ID')
output <- data.table(matrix(ncol = 4, nrow = 0))
colnames(output) <- columnas_out
output <- bind_rows(output, parte_A)
output$`ID de Usuario` <- output$`ID DE USUARIO`
output$`Nombre de pila` <- output$`Nombre de usuario`
output$Apellidos <- output$`Apellido de usuario`
output$`ID DE USUARIO` <- NULL
output$`Nombre de usuario` <- NULL
output$`Apellido de usuario` <- NULL


#Eliminamos las vacantes que ya han caducado
parte_B$`Fecha de publicación de la oferta de empleo` <-
    dmy_hm(parte_B$`Fecha de publicación de la oferta de empleo`)
parte_B$`Publicación - Fecha de caducidad` <-
    dmy(parte_B$`Publicación - Fecha de caducidad`)
parte_B <-
    parte_B %>% filter(
        `Publicación - Fecha de caducidad` >= today("GMT") |
            is.na(`Publicación - Fecha de caducidad`)
    )

# Obtenemos las 10 primersas vacantes que concuerdan con las preferencias y las grabamos en el
# data frame de salida

datos <- data.table()
for (i in 1:nrow(parte_B)) {
    d <- filter(
        parte_A,
        (
            parte_A$'Mis Preferencias para ofertas de empleo (Opción 1)' == parte_B[i,]$`Tus preferencias`
        ) |
            (
                parte_A$'Mis Preferencias para ofertas de empleo (Opción 2)' == parte_B[i,]$`Tus preferencias`
            ) |
            (
                parte_A$'Mis Preferencias para ofertas de empleo (Opción 3)' == parte_B[i,]$`Tus preferencias`
            ) |
            (
                parte_A$'Mis Preferencias para ofertas de empleo (Opción 4)' == parte_B[i,]$`Tus preferencias`
            )
    ) %>%
        select("ID DE USUARIO") %>%
        merge(parte_B[i,])
    datos <- rbind(datos, d)
}
datos <-
    datos[order(
        datos$"ID DE USUARIO",
        datos$`Publicación - Fecha de caducidad`,
        datos$`Número de candidatos de la oferta de empleo`
    ),]
d <- datos %>% group_by(datos$`ID DE USUARIO`) %>%  slice(1:10) %>%
    mutate(vacante = paste("TP- ID de la vacante ", row_number(), sep =
                               "")) %>%
    mutate(titulo = paste("TP- Titulo de la vacante ", row_number(), sep =
                              "")) %>%
    mutate(localidad = paste("TP- Localidad de la vacante ", row_number(), sep =
                                 ""))
d$`Tus preferencias` <- NULL
d$`Fecha de publicación de la oferta de empleo` <- NULL
d$`Publicación - Fecha de caducidad` <- NULL
d <- ungroup(d)
d$"datos$`ID DE USUARIO`" <- NULL
d$`Número de candidatos de la oferta de empleo` <- NULL
d$`Publicación de la oferta de empleo - Centro Profesional` <- NULL

#para cada empleado obtenemos las vacantes que ya están en la primera  columnas
excluir <- d %>% select("ID DE USUARIO", `Id. de oferta de empleo`)

d <- d %>% group_by(`ID DE USUARIO`) %>%
    spread(vacante, `Id. de oferta de empleo`) %>%
    spread(titulo, "Mostrar título del puesto de la oferta de empleo") %>%
    spread(localidad, "Lugar de la oferta de empleo") %>%
    summarise_all(function (x)
        first(na.omit(x)))
output <-
    left_join(output, d, by = c("ID de Usuario" = "ID DE USUARIO"))



# Obtenemos las 10 primeras vacantes a punto de cerrar: fecha de caducidad <= hoy + 3 días

datos <- data.table()
apdc <-
    parte_B %>% filter(`Publicación - Fecha de caducidad` <= (today("GMT") +
                                                                  5))
for (i in 1:nrow(apdc)) {
    d <- filter(
        parte_A,
        (
            parte_A$'Mis Preferencias para ofertas de empleo (Opción 1)' != apdc[i,]$`Tus preferencias`
        ) &
            (
                parte_A$'Mis Preferencias para ofertas de empleo (Opción 2)' != apdc[i,]$`Tus preferencias`
            ) &
            (
                parte_A$'Mis Preferencias para ofertas de empleo (Opción 3)' != apdc[i,]$`Tus preferencias`
            ) &
            (
                parte_A$'Mis Preferencias para ofertas de empleo (Opción 4)' != apdc[i,]$`Tus preferencias`
            )
    ) %>%
        select("ID DE USUARIO") %>%
        merge(apdc[i,])
    datos <- rbind(datos, d)
}

datos <-
    datos[order(
        datos$"ID DE USUARIO",
        datos$`Publicación - Fecha de caducidad`,
        datos$`Número de candidatos de la oferta de empleo`
    ),]
d <- datos %>% group_by(datos$`ID DE USUARIO`) %>%  slice(1:10) %>%
    mutate(vacante = paste("APD- ID de la vacante ", row_number(), sep =
                               "")) %>%
    mutate(titulo = paste("APD- Titulo de la vacante ", row_number(), sep =
                              "")) %>%
    mutate(localidad = paste("APD- Localidad de la vacante ", row_number(), sep =
                                 ""))
d$`Tus preferencias` <- NULL
d$`Fecha de publicación de la oferta de empleo` <- NULL
d$`Publicación - Fecha de caducidad` <- NULL
d <- ungroup(d)
d$"datos$`ID DE USUARIO`" <- NULL
d$`Número de candidatos de la oferta de empleo` <- NULL
d$`Publicación de la oferta de empleo - Centro Profesional` <- NULL

#para cada empleado obtenemos las vacantes que ya están en la tercera columnas
excluir_apdc <-
    d %>% select("ID DE USUARIO", `Id. de oferta de empleo`)
excluir <- rbind(excluir, excluir_apdc)

d <- d %>% group_by(`ID DE USUARIO`) %>%
    spread(vacante, `Id. de oferta de empleo`) %>%
    spread(titulo, "Mostrar título del puesto de la oferta de empleo") %>%
    spread(localidad, "Lugar de la oferta de empleo") %>%
    summarise_all(function (x)
        first(na.omit(x)))
output <-
    left_join(output, d, by = c("ID de Usuario" = "ID DE USUARIO"))



# Obtenemos las 10 primeras vacantes del resto de vacantes que no se muestran ni en la primera
# ni en la tercera columna
datos <- data.table()
for (i in 1:nrow(parte_B)) {
    d <- parte_A %>%
        select("ID DE USUARIO") %>%
        merge(parte_B[i,])
    datos <- rbind(datos, d)
}
 

#quitamos todos aquellos pares ID DE USUARIO - VACANTE que estén en la lista a excluir
datos <- datos %>% anti_join(excluir)

datos <-
    datos[order(
        datos$"ID DE USUARIO",
        datos$`Publicación - Fecha de caducidad`,
        datos$`Número de candidatos de la oferta de empleo`
    ),]
d <- datos %>% group_by(datos$`ID DE USUARIO`) %>%  slice(1:10) %>%
    mutate(vacante = paste("OV- ID de la vacante ", row_number(), sep =
                               "")) %>%
    mutate(titulo = paste("OV- Titulo de la vacante ", row_number(), sep =
                              "")) %>%
    mutate(localidad = paste("OV- Localidad de la vacante ", row_number(), sep =
                                 ""))
d$`Tus preferencias` <- NULL
d$`Fecha de publicación de la oferta de empleo` <- NULL
d$`Publicación - Fecha de caducidad` <- NULL
d <- ungroup(d)
d$"datos$`ID DE USUARIO`" <- NULL
d$`Número de candidatos de la oferta de empleo` <- NULL
d$`Publicación de la oferta de empleo - Centro Profesional` <- NULL

d <- d %>% group_by(`ID DE USUARIO`) %>%
    spread(vacante, `Id. de oferta de empleo`) %>%
    spread(titulo, "Mostrar título del puesto de la oferta de empleo") %>%
    spread(localidad, "Lugar de la oferta de empleo") %>%
    summarise_all(function (x)
        first(na.omit(x)))
output <-
    left_join(output, d, by = c("ID de Usuario" = "ID DE USUARIO"))
output$`Mis Preferencias para ofertas de empleo (Opción 1)` <- NULL
output$`Mis Preferencias para ofertas de empleo (Opción 2)` <- NULL
output$`Mis Preferencias para ofertas de empleo (Opción 3)` <- NULL
output$`Mis Preferencias para ofertas de empleo (Opción 4)` <- NULL

fwrite(output, file = paste("results/macro_test_jorge", ".csv", sep = ""))

toc()
