library("lubridate")
library("uuid")
library("base64enc")
library("digest")
library("httr")
library("xml2")
library("RJSONIO")
library("jsonlite")
library("data.table")


timeStampCSOD <- function () {
    return (format(Sys.time(), "%Y-%m-%dT%H:%M:%S.000"))
}

firmarCadenaSesion <- function(cadena, secret) {
    # Firmamos la cadena con el sessionSecret decodificado
    hash <- hmac(secret, cadena, "sha512", raw = TRUE)
    # Codificamos la cadena a Base64
    return(base64encode(hash))
}

leerOdata <- function (vista, token, secret, parametros) {
    # fecha actual UTC en formato ISO con milisegundos a 000 tal y como requiere Cornerstone
    isoDate <- timeStampCSOD()
    
    # Se construye la cadena a firmar
    httpMethod <- "GET"
    
    httpUrl <-
        paste("/services/api/x/odata/api/views/",
              vista ,
              parametros,
              sep = "")
    stringToSign <-
        paste(
            httpMethod,
            "\n",
            "x-csod-date:",
            isoDate,
            "\n",
            "x-csod-session-token:",
            token,
            "\n",
            httpUrl,
            sep = ""
        )
    
    cadenaFirmada <- firmarCadenaSesion(stringToSign, secret)
    # Invocamos al API
    url <-
        paste(entorno,
              "/services/api/x/odata/api/views/",
              vista,
              parametros,
              sep = "")
    # print(url)
    odata <- GET(
        url,
        add_headers("x-csod-date" = isoDate),
        add_headers("x-csod-session-token" = token),
        add_headers("x-csod-signature" = cadenaFirmada)
    )
    
    # print (odata$status_code)
    # print(odata)
    datos <-
        fromJSON(content(odata, type = "text/json", encoding = "UTF-8"))
    
    
    df <- datos$value
    # print(nrow(df))
    siguiente_link <- datos$"@odata.nextLink"
    # print(siguiente_link)
    while (!is.null(datos$"@odata.nextLink")) {
        url <- paste(siguiente_link, sep = "")
        
        odata <- GET(
            url,
            add_headers("x-csod-date" = isoDate),
            add_headers("x-csod-session-token" = token),
            add_headers("x-csod-signature" = cadenaFirmada)
        )
        
        
        datos <-
            fromJSON(content(odata, type = "text/json", encoding = "UTF-8"))
        df <- rbind(df, datos$value)
        # print(nrow(df))
        siguiente_link <- datos$"@odata.nextLink"
        # print(siguiente_link)
        
    }
    return(df)
}


leermetadatos <- function (token, secret) {
    # fecha actual UTC en formato ISO con milisegundos a 000 tal y como requiere Cornerstone
    isoDate <- timeStampCSOD()
    
    # Se construye la cadena a firmar
    httpMethod <- "GET"
    
    httpUrl <-
        paste("/services/api/x/odata/api/views/", sep = "")
    
    stringToSign <-
        paste(
            httpMethod,
            "\n",
            "x-csod-date:",
            isoDate,
            "\n",
            "x-csod-session-token:",
            token,
            "\n",
            httpUrl,
            sep = ""
        )
    cadenaFirmada <- firmarCadenaSesion(stringToSign, secret)
    # Invocamos al API
    url <- paste(entorno,
                 "/services/api/x/odata/api/views",
                 sep = "")
    
    odata <- GET(
        url,
        #    add_headers("prefer" = "odata.maxpagesize=500"),
        add_headers("x-csod-date" = isoDate),
        add_headers("x-csod-session-token" = token),
        add_headers("x-csod-signature" = cadenaFirmada)
    )
    return(content(odata, "text", encoding = "UTF-8"))
}

obtenerTokenSesion <- function(apiSecret, apiKey) {
    # fecha actual UTC en formato ISO con milisegundos a 000 tal y como requiere Cornerstone
    isoDate <- timeStampCSOD()
    
    alias <- UUIDgenerate()
    
    # Se construye la cadena a firmar
    httpMethod <- "POST"
    
    httpUrl <- "/services/api/sts/session"
    
    stringToSign <-
        paste(
            httpMethod,
            "\n",
            "x-csod-api-key:",
            apiKey,
            "\n",
            "x-csod-date:",
            isoDate,
            "\n",
            httpUrl,
            sep = ""
        )
    
    # El apiSecret está codificado en Base64, lo decodificamos
    secretKey <- base64decode(apiSecret)
    
    
    # Firmamos la cadena con el apiSecret decodificado
    hash <- hmac(secretKey, stringToSign, "sha512", raw = TRUE)
    
    # Codificamos la cadena a Base64
    signature <- base64encode(hash)
    
    # Invocamos al API para obtener el token de sesión usando rcurl
    url <-
        paste(entorno,
              "/services/api/sts/session?userName=",
              user,
              "&alias=",
              alias,
              sep = "")
    
    
    a <- POST(
        url,
        accept_json(),
        add_headers("content-type" = "text/xml"),
        add_headers("content-length" = "0"),
        add_headers("x-csod-api-key" = apiKey),
        add_headers("x-csod-date" = isoDate),
        add_headers("x-csod-signature" = signature)
    )
    
    sessionToken <- content(a)$data[[1]]$Token
    sessionSecret <- content(a)$data[[1]]$Secret
    return(
        list(
            sessionToken = sessionToken,
            sessionSecret =  sessionSecret,
            sessionSecretKey = base64decode(sessionSecret)
        )
    )
    
}