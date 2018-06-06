source("reportingAPI.R")
source("clavesCSOD_pro.R")


#function obtenerParteB() {
    #Devuelve un data frame con la información de vacantes publicadas en el career center
    sesion <- obtenerTokenSesion(apiSecret, apiKey)
    vw_rpt_recruiting <-
        leerOdata("vw_rpt_recruiting",
                  sesion$sessionToken,
                  sesion$sessionSecretKey,
                  "")
    vw_rpt_requisition <-
        leerOdata("vw_rpt_requisition",
                  sesion$sessionToken,
                  sesion$sessionSecretKey,
                  "")
    vw_rpt_requisition_posting <-
        leerOdata("vw_rpt_requisition_posting",
                  sesion$sessionToken,
                  sesion$sessionSecretKey,
                  "")
    parte_B <- vw_rpt_recruiting %>% filter(ats_req_status=="Open",
                                            ats_req_posting_career_center=="Yes"
                                            ) %>% 
        left_join(vw_rpt_requisition_posting,by = c("ats_req_id" = "posting_job_requisition_id")) 
        
        
#}