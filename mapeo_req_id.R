source("reportingAPI.R")
source("clavesCSOD_pro.R")
library(dplyr)


sesion <- obtenerTokenSesion(apiSecret, apiKey)

vw_rpt_recruiting <-leerOdata("vw_rpt_recruiting", sesion$sessionToken, sesion$sessionSecretKey, "")

# mapeo <- vw_rpt_recruiting %>% select(ats_req_ref,ats_req_id) %>%
# filter(substr(ats_req_ref,4,length(ats_req_ref)) != ats_req_id)
# 
# fwrite(mapeo, file = paste("mapeo_req_id", ".csv", sep = ""))

datos <- fread("Landing_Page_Input_B_19_14_56.txt", encoding = "UTF-8")
columnas <- names(datos)
datos <- merge(datos, mapeo, by.x = "Id. de oferta de empleo", by.y="ats_req_ref", all.x = TRUE)
datos$`Id. de oferta de empleo` <- datos$ats_req_id
datos$ats_req_id <- NULL
datos <- datos[,columnas,with=FALSE]

fwrite(datos, file = paste("mapeo_req_id", ".csv", sep = ""))
