source("reportingAPI.R")
source("clavesCSOD_pro.R")
library(dplyr)
library(data.table)




#function obtenerParteB() {
#Devuelve un data frame con la información de vacantes publicadas en el career center
sesion <- obtenerTokenSesion(apiSecret, apiKey)
vw_rpt_recruiting <-
    leerOdata("vw_rpt_recruiting",
              sesion$sessionToken,
              sesion$sessionSecretKey,
              "?$select=ats_req_id,ats_req_status,ats_req_posting_career_center,ats_req_candidates_number")
vw_rpt_requisition_posting <-
    leerOdata("vw_rpt_requisition_posting",
              sesion$sessionToken,
              sesion$sessionSecretKey,
              "")
vw_rpt_requisition_location <-
    leerOdata("vw_rpt_requisition_location",
              sesion$sessionToken,
              sesion$sessionSecretKey,
              "")
vw_rpt_job_requisition_local    <-
    leerOdata("vw_rpt_job_requisition_local",
              sesion$sessionToken,
              sesion$sessionSecretKey,
              "?$filter=culture_id%20eq%201")
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
    left_join(vw_rpt_job_requisition_local, by = c("ats_req_id" = "jrl_job_requisition_id")) %>%
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
ubicaciones <- data.table()
for (i in unique(parte_B$ats_multi_loc_req_loc_id)) {
    parametros <-paste0("?$filter=(ou_id%20eq%20",i,")and(culture_id%20eq%201)")
    u  <-
        leerOdata("vw_rpt_ou_title_local",
                  sesion$sessionToken,
                  sesion$sessionSecretKey,
                  parametros)
    ubicaciones<-rbind(ubicaciones,u)
}
parte_B <-
    left_join(parte_B,
          ubicaciones,
          by = c("ats_multi_loc_req_loc_id" =  "ou_id"))


    

    [1] "Tus preferencias"                                       
    [2] "Mostrar título del puesto de la oferta de empleo"       
    [3] "Id. de oferta de empleo"                                
    [4] "Fecha de publicación de la oferta de empleo"            
    [5] "Publicación - Fecha de caducidad"                       
    [6] "Publicación de la oferta de empleo - Centro Profesional"
    [7] "Número de candidatos de la oferta de empleo"            
    [8] "Lugar de la oferta de empleo" 
    "País"
    
    
            
#} [1] "ats_req_app_wfl_template"          
    [2] "ats_req_app_wfl_template_id"       
    [3] "ats_req_compensation_type"         
    [4] "ats_req_currency"                  
    [5] "ats_req_eeo_cat"                   
    [6] "ats_req_full_part"                 
    [7] "ats_req_priority"                  
    [8] "ats_req_status"                    
    [9] "ats_req_template_ref"              
    [10] "ats_req_primary_owner_id"          
    [11] "ats_req_request_review_dt"         
    [12] "ats_req_request_created_dt"        
    [13] "ats_req_request_creator_id"        
    [14] "ats_req_compensation_type_id"      
    [15] "ats_contact_phone"                 
    [16] "ats_req_compensation_high"         
    [17] "ats_req_compensation_low"          
    [18] "ats_req_currency_id"               
    [19] "ats_req_opened_dt"                 
    [20] "ats_req_days_open"                 
    [21] "ats_req_eeo_cat_id"                
    [22] "ats_req_empl_type_id"              
    [23] "ats_req_full_part_id"              
    [24] "ats_req_openings_org"              
    [25] "ats_req_ongoing"                   
    [26] "ats_req_posting_career_center"     
    [27] "ats_req_jb_show_salary_external"   
    [28] "ats_req_ref"                       
    [29] "ats_req_priority_id"               
    [30] "ats_req_status_id"                 
    [31] "ats_req_target_hire_dt"            
    [32] "ats_req_time_to_fill"              
    [33] "ats_req_hiring_manager_id"         
    [34] "ats_req_can_apply"                 
    [35] "ats_req_closed_dt"                 
    [36] "ats_requisition_template_id"       
    [37] "ats_req_parent_req_ref"            
    [38] "ats_req_id"                        
    [39] "ats_req_filled_dt"                 
    [40] "ats_req_candidates_number"         
    [41] "ats_req_suggested_referrals_number"
    [42] "ats_req_openings_outstanding"      
    [43] "ats_req_postings_number"           
    [44] "ats_req_submission_number"         
    [45] "ats_req_attached_documents"        
    [46] "ats_req_div_id"                    
    [47] "ats_req_position_id"               
    [48] "ats_req_grade_id"                  
    [49] "ats_req_type_id"                   
    [50] "cost_center_id"                    
    [51] "ats_req_position_ref"              
    [52] "ats_req_division_ref"              
    [53] "ats_req_grade_ref"                 
    [54] "ats_req_cost_center_ref"           
    [55] "ats_req_bonus_amount"              
    [56] "ats_req_created_dt"                
    [57] "ats_req_hiring_manager"            
    [58] "ats_req_request_creator_full_name" 
    [59] "ats_req_primary_owner_full_name"   
    [60] "ats_req_id_pk"                     
    [61] "ats_req_primary_owner_ref"         
    [62] "ats_req_request_creator_ref"       
    [63] "ats_req_hiring_manager_ref"        
    [64] "ats_req_primary_owner_email"       
    [65] "ats_req_hiring_manager_email"      
    [66] "posting_job_board_id"              
    [67] "posting_date"                      
    [68] "posting_jb_language_id"            
    [69] "posting_jb_language_name"          
    [70] "posting_jb_number_of_clicks"       
    [71] "posting_expiration_date"           
    [72] "posting_type" 