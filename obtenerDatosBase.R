source("reportingAPI.R")
source("clavesCSOD_pro.R")

library(dplyr)
library(data.table)
library(tictoc)




obtenerParteB <- function() {
    #Devuelve un data frame con la información de vacantes publicadas en el career center
    sesion <- obtenerTokenSesion(apiSecret, apiKey)
    listaVistas <- fromJSON(leermetadatos(sesion$sessionToken,sesion$sessionSecretKey))
    
        vw_rpt_recruiting <-
        leerOdata(
            "vw_rpt_recruiting",
            sesion$sessionToken,
            sesion$sessionSecretKey,
            "?$select=ats_req_id,ats_req_status,ats_req_posting_career_center,ats_req_candidates_number"
        )
    vw_rpt_requisition_posting <-
        leerOdata(
            "vw_rpt_requisition_posting",
            sesion$sessionToken,
            sesion$sessionSecretKey,
            ""
        )
    vw_rpt_requisition_location <-
        leerOdata(
            "vw_rpt_requisition_location",
            sesion$sessionToken,
            sesion$sessionSecretKey,
            ""
        )
    vw_rpt_job_requisition_local    <-
        leerOdata(
            "vw_rpt_job_requisition_local",
            sesion$sessionToken,
            sesion$sessionSecretKey,
            "?$filter=culture_id%20eq%2015"
        )
    parte_B <-
        vw_rpt_recruiting %>% filter(ats_req_status == "Open",
                                     ats_req_posting_career_center ==
                                         "Yes") %>%
        left_join(vw_rpt_requisition_posting,
                  by = c("ats_req_id" = "posting_job_requisition_id")) %>%
        filter(posting_type == "Career Center") %>%
        left_join(vw_rpt_requisition_location,
                  by = c("ats_req_id" = "ats_multi_loc_req_id"))  %>%
        filter(ats_multi_loc_is_primary == TRUE) %>%
        left_join(vw_rpt_job_requisition_local,
                  by = c("ats_req_id" = "jrl_job_requisition_id")) %>%
        select(
            ats_req_id,
            posting_date,
            posting_expiration_date,
            ats_req_posting_career_center,
            ats_req_candidates_number,
            ats_multi_loc_country,
            ats_multi_loc_req_ref,
            ats_multi_loc_req_loc_id,
            jrl_title
        )
    #Obtener el nombre de la ubicación
    ubicaciones <- data.table()
    for (i in unique(parte_B$ats_multi_loc_req_loc_id)) {
        parametros <-
            paste0("?$filter=(ou_id%20eq%20",
                   i,
                   ")and(culture_id%20eq%2015)")
        u  <-
            leerOdata(
                "vw_rpt_ou_title_local",
                sesion$sessionToken,
                sesion$sessionSecretKey,
                parametros
            )
        ubicaciones <- rbind(ubicaciones, u)
    }
    parte_B <-
        left_join(parte_B,
                  ubicaciones,
                  by = c("ats_multi_loc_req_loc_id" =  "ou_id"))
    
    #Obtener el campo "Tus preferencias"
    vw_rpt_requisition_cf    <-
        leerOdata(
            "vw_rpt_requisition_cf",
            sesion$sessionToken,
            sesion$sessionSecretKey,
            "?$select=oucf_id,ou_custom_field_01360"
        )
    parte_B <- left_join(parte_B,
                         vw_rpt_requisition_cf,
                         by = c("ats_req_id" =  "oucf_id"))
    vw_rpt_custom_field_value_local <-
        leerOdata(
            "vw_rpt_custom_field_value_local",
            sesion$sessionToken,
            sesion$sessionSecretKey,
            "?$filter=culture_id%20eq%2015"
        )
    parte_B <- left_join(
        parte_B,
        vw_rpt_custom_field_value_local,
        by = c("ou_custom_field_01360" =  "cfvl_value_id")
    )
    
    parte_B <- parte_B %>%
        select(
            cfvl_title,
            jrl_title,
            ats_req_id,
            posting_date,
            posting_expiration_date,
            ats_req_posting_career_center,
            ats_req_candidates_number,
            title,
            ats_multi_loc_country
        ) %>%
        filter(!is.na(cfvl_title))
    colnames(parte_B) <-
        c(
            "Tus preferencias" ,
            "Mostrar título del puesto de la oferta de empleo"  ,
            "Id. de oferta de empleo"              ,
            "Fecha de publicación de la oferta de empleo"  ,
            "Publicación - Fecha de caducidad"           ,
            "Publicación de la oferta de empleo - Centro Profesional",
            "Número de candidatos de la oferta de empleo",
            "Lugar de la oferta de empleo" ,
            "País de la oferta de empleo"
        )
    
    return(parte_B)
}



obtenerParteA <- function() {
    #Devuelve un data frame con la información de los empleados a los cuales cargar vacantes en la landing page
    sesion <- obtenerTokenSesion(apiSecret, apiKey)
    vw_rpt_ou_type <-
        leerOdata(
            "vw_rpt_ou_type",
            sesion$sessionToken,
            sesion$sessionSecretKey,
            ""
        )
    vw_rpt_ou <-
        leerOdata(
            "vw_rpt_ou",
            sesion$sessionToken,
            sesion$sessionSecretKey,
            "?$filter=type_id%20eq%20128"
    
            )
    vw_rpt_user_ou <-
        leerOdata(
            "vw_rpt_user_ou",
            sesion$sessionToken,
            sesion$sessionSecretKey,
            "?$filter=(ou_id%20eq%20353418)or(ou_id%20eq%20353417)"
        )
    
    vw_rpt_user <-
        leerOdata(
            "vw_rpt_user",
            sesion$sessionToken,
            sesion$sessionSecretKey,
            "?$select=user_id,user_name_first,user_name_last,user_div_ref,user_country&$filter=user_status_id%20eq%201"
        )

    vw_rpt_career_pref <-
        leerOdata(
            "vw_rpt_career_pref",
            sesion$sessionToken,
            sesion$sessionSecretKey,
            ""
        )
    
    vw_rpt_resume <-
        leerOdata(
            "vw_rpt_resume",
            sesion$sessionToken,
            sesion$sessionSecretKey,
            "?$top=10000"
        )    
    
    vw_rpt_resume
    vw_rpt_career_pref[vw_rpt_career_pref$career_pref_question_6 == "10",13] <- "ENGINEERING"
    
    "ENGINEERING"
    "FINANCE" 
    "LEGAL & COMPLIANCE"
    "RISK MANAGEMENT"
    "COMMUNICATIONS & MARKETING"
    "TALENT & CULTURE"
    "BANCA MINORISTA/RETAIL/RED"  
    "BANCA DE EMPRESAS,INSTITUCIONES,CORPORATIVAS"
    "DATA" 
    "STRATEGY, M&A Y REAL STATE"
    "AUDIT" 
    "CUSTOMER & CLIENT SOLUTIONS"  
    "CIB"
    "SELECCIONAR"
    

    
    
    
    return(parte_A)
}
