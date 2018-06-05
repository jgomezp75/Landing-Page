source("reportingAPI.R")
source("clavesCSOD_pro.R")

# Calculate the number of cores
no_cores <- detectCores() - 1
# Initiate cluster
cl <- makeCluster(no_cores)
registerDoParallel(cl)

sesion <- obtenerTokenSesion(apiSecret, apiKey)
listaVistas <- fromJSON(leermetadatos(sesion$sessionToken,sesion$sessionSecretKey))
vistas <- listaVistas$value$url


d<-leerOdata("vw_rpt_applicant", sesion$sessionToken, sesion$sessionSecretKey, "")
fwrite(d, file = paste("vw_rpt_applicant", ".csv", sep = ""))

d<-leerOdata("vw_rpt_requisition_cf", sesion$sessionToken, sesion$sessionSecretKey,"")
fwrite(d, file = paste("vw_rpt_requisition_cf", ".csv", sep = ""))



for (v in 1:length(vistas)) {
    if (!file.exists(paste(vistas[v], ".csv", sep = ""))) {
        print(paste("Descargando", vistas[v]))
        d<-leerOdata(vistas[v], sesion$sessionToken, sesion$sessionSecretKey)
        fwrite(d, file = paste(vistas[v], ".csv", sep = ""))
    }
}

# foreach(v = 1:length(vistas), .packages=c("digest","base64enc","httr","jsonlite","data.table","xml2")) %dopar% {
#     if (!file.exists(paste(vistas[v], ".csv", sep = ""))) {
#         d<-leerOdata(vistas[v], sesion$sessionToken, sesion$sessionSecretKey)
#         fwrite(d, file = paste(vistas[v], ".csv", sep = ""))
#     }
# }

stopImplicitCluster()
