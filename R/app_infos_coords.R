
#' Read AWS metadata.
#'
#' Read AWS coordinates and parameters.
#' 
#' @param aws_dir full path to the directory containing ADT.\cr
#'               Example: "D:/ZMD_AWS_v2"
#' 
#' @return a JSON object
#' 
#' @export

readCoords <- function(aws_dir){
    parsFile <- file.path(aws_dir, "AWS_DATA", "JSON", "aws_parameters.json")
    awsPars <- jsonlite::read_json(parsFile)
    crds <- readCoordsData(aws_dir)
    id <- sapply(awsPars, '[[', 'id')
    ix <- match(id, crds$id)

    awsPars <- lapply(seq_along(id), function(j){
        x <- awsPars[[j]]
        y <- as.list(crds[ix[j], ])
        nx <- names(x)
        nx <- nx[nx %in% names(y)]
        for(n in nx) x[[n]] <- y[[n]]
        x$startdate <- y$startdate
        x$enddate <- y$enddate

        return(x)
    })

    return(convJSON(awsPars))
}

#############
#' Get AWS coordinates.
#'
#' Get AWS coordinates to display on map.
#' 
#' @param aws_dir full path to the directory containing ADT.\cr
#'               Example: "D:/ZMD_AWS_v2"
#' 
#' @return a JSON object
#' 
#' @export

readCoordsMap <- function(aws_dir){
    crds <- readCoordsData(aws_dir)
    return(convJSON(crds))
}

readCoordsData <- function(aws_dir){
    tz <- Sys.getenv("TZ")
    origin <- "1970-01-01"
    netNOM <- c("Campbell", "Adcon")
    netKOLS <- c("blue", "green")
    netCRDS <- c("campbell_crds", "adcon_crds")
    nmCol <- c("id", "name", "longitude", "latitude", "altitude", "network",
               "network_code", "province", "district", "startdate", "enddate")

    #############

    adt_args <- readRDS(file.path(aws_dir, "AWS_DATA", "AUTH", "adt.con"))
    con_adt <- try(connect.database(adt_args$connection,
                   RMySQL::MySQL()), silent = TRUE)
    if(inherits(con_adt, "try-error")){
        return(convJSON(NULL))
    }

    crds <- lapply(seq_along(netNOM), function(j){
        crd <- DBI::dbReadTable(con_adt, netCRDS[j])
        crd$network <- netNOM[j]
        crd$network_code <- j

        return(crd)
    })

    DBI::dbDisconnect(con_adt)

    crds <- lapply(crds, function(x) x[, nmCol, drop = FALSE])
    crds <- do.call(rbind, crds)

    #############
    crds$startdate <- as.POSIXct(as.integer(crds$startdate), origin = origin, tz = tz)
    crds$startdate <- format(crds$startdate, "%Y-%m-%d %H:%M")
    crds$startdate[is.na(crds$startdate)] <- ""

    crds$enddate <- as.POSIXct(as.integer(crds$enddate), origin = origin, tz = tz)
    crds$enddate <- format(crds$enddate, "%Y-%m-%d %H:%M")
    crds$enddate[is.na(crds$enddate)] <- ""

    #############
    xcrd <- crds[, c('longitude', 'latitude')]
    xcrd <- paste(xcrd[, 1], xcrd[, 2], sep = "_")
    ix1 <- duplicated(xcrd) & !is.na(crds$longitude)
    ix2 <- duplicated(xcrd, fromLast = TRUE) & !is.na(crds$longitude)
    ix <- ix1 | ix2
    icrd <- unique(xcrd[ix])

    #############

    crds <- apply(crds, 2, as.character)
    crds <- cbind(crds, StatusX = "blue")

    for(j in seq_along(netNOM))
        crds[crds[, "network"] == netNOM[j], "StatusX"] <- netKOLS[j]

    #############
    if(length(icrd) > 0){
        for(jj in icrd){
            ic <- xcrd == jj
            xx <- apply(crds[ic, ], 2, paste0, collapse = " | ")
            xx <- matrix(xx, nrow = 1, dimnames = list(NULL, names(xx)))
            xx <- do.call(rbind, lapply(seq_along(which(ic)), function(i) xx))

            xcr <- crds[ic, c('longitude', 'latitude')]
            crds[ic, ] <- xx
            crds[ic, c('longitude', 'latitude')] <- xcr
            crds[ic, 'StatusX'] <- "red"
        }
    }

    #############
    crds[is.na(crds)] <- ""
    crds <- cbind(crds, LonX = crds[, 3], LatX = crds[, 4])
    ix <- crds[, 'LonX'] == "" | crds[, 'LatX'] == ""
    crds[ix, c('LonX', 'LatX')] <- NA
    crds <- as.data.frame(crds)
    crds$LonX <- as.numeric(as.character(crds$LonX))
    crds$LatX <- as.numeric(as.character(crds$LatX))

    #############
    # get parameters for each aws
    # crds$PARS <- pars

    return(crds)
}

#############
#' Get AWS coordinates for one network.
#'
#' Get AWS coordinates for one network to display on table.
#' 
#' @param network the AWS network code;
#'  1: campbell, 2: adcon.
#' @param aws_dir full path to the directory containing ADT.\cr
#'               Example: "D:/ZMD_AWS_v2"
#' 
#' @return a JSON object
#' 
#' @export

tableAWSCoords <- function(network, aws_dir){
    tz <- Sys.getenv("TZ")
    origin <- "1970-01-01"
    AWS_CRDS <- c("campbell_crds", "adcon_crds")

    #############
    adt_args <- readRDS(file.path(aws_dir, "AWS_DATA", "AUTH", "adt.con"))
    con_adt <- try(connect.database(adt_args$connection,
                   RMySQL::MySQL()), silent = TRUE)
    if(inherits(con_adt, "try-error")){
        status <- data.frame(status = "unable to connect to database")
        return(convJSON(status))
    }

    #############
    awsnet <- AWS_CRDS[as.integer(network)]
    crds <- DBI::dbReadTable(con_adt, awsnet)
    DBI::dbDisconnect(con_adt)

    #############
    crds$startdate <- as.POSIXct(as.integer(crds$startdate), origin = origin, tz = tz)
    crds$startdate <- format(crds$startdate, "%Y-%m-%d %H:%M")
    crds$startdate[is.na(crds$startdate)] <- ""

    crds$enddate <- as.POSIXct(as.integer(crds$enddate), origin = origin, tz = tz)
    crds$enddate <- format(crds$enddate, "%Y-%m-%d %H:%M")
    crds$enddate[is.na(crds$enddate)] <- ""

    #############

    return(convJSON(crds))
}

#############
#' Get AWS start and end time.
#'
#' Get the start and end time of a specified AWS.
#' 
#' @param id ID of the AWS.
#' @param network the AWS network code; 
#' 1: campbell, 2: adcon.
#' @param aws_dir full path to the directory containing ADT.\cr
#'               Example: "D:/ZMD_AWS_v2"
#' 
#' @return a JSON object
#' 
#' @export

### to be integrate to readCoords
getAWSTimeRange <- function(id, network, aws_dir){
    tz <- Sys.getenv("TZ")
    origin <- "1970-01-01"
    AWS_CRDS <- c("campbell_crds", "adcon_crds")

    #############
    adt_args <- readRDS(file.path(aws_dir, "AWS_DATA", "AUTH", "adt.con"))
    con_adt <- try(connect.database(adt_args$connection,
                   RMySQL::MySQL()), silent = TRUE)
    if(inherits(con_adt, "try-error")){
        return(convJSON(NULL))
    }

    net_dat <- AWS_CRDS[as.integer(network)]

    query <- paste0("SELECT startdate, enddate FROM ", net_dat, " WHERE id='", id, "'")
    qres <- DBI::dbGetQuery(con_adt, query)
    DBI::dbDisconnect(con_adt)
    qres <- lapply(qres, as.POSIXct, origin = origin, tz = tz)
    qres <- lapply(qres, format, "%Y-%m-%d %H:%M:%S")

    return(convJSON(qres))
}

#############
#' Get AWS Wind coordinates.
#'
#' Get AWS Wind coordinates to display on map.
#' 
#' @param height wind speed and direction heights above ground, format "<speedHeight>_<directionHeight>".
#' @param aws_dir full path to the directory containing ADT.\cr
#'               Example: "D:/ZMD_AWS_v2"
#' 
#' @return a JSON object
#' 
#' @export

readCoordsWind <- function(height, aws_dir){
    parsFile <- file.path(aws_dir, "AWS_DATA", "JSON", "aws_parameters.json")
    awsPars <- jsonlite::read_json(parsFile)
    height <- strsplit(height, "_")
    ws_val <- height[[1]][1]
    wd_val <- height[[1]][2]

    coordAWS <- lapply(awsPars, function(x){
        if(all(9:10 %in% unlist(x$PARS))){
            dd <- wd_val %in% names(x$height[['9']][[1]])
            ff <- ws_val %in% names(x$height[['10']][[1]])
            if(dd & ff){
                aws <- x[c("network_code", "network", "id", "name")]
                return(aws)
            }
        }

        return(NULL)
    })

    inull <- sapply(coordAWS, is.null)
    coordAWS <- coordAWS[!inull]

    return(convJSON(coordAWS))
}

#############
#' Get AWS Wind heights.
#'
#' Get AWS Wind heights for dropdown select .
#' 
#' @param aws_dir full path to the directory containing ADT.\cr
#'               Example: "D:/ZMD_AWS_v2"
#' 
#' @return a JSON object
#' 
#' @export

getWindHeight <- function(aws_dir){
    parsFile <- file.path(aws_dir, "AWS_DATA", "JSON", "aws_parameters.json")
    awsPars <- jsonlite::read_json(parsFile)

    wndHgt <- lapply(awsPars, function(x){
        if(all(9:10 %in% unlist(x$PARS))){
            dd_h <- sort(as.numeric(do.call(c, x$height[['9']][[1]])))
            ff_h <- sort(as.numeric(do.call(c, x$height[['10']][[1]])))
            if(length(ff_h) > length(dd_h)){
                dd <- sapply(ff_h, function(v){
                    ii <- which.min(abs(dd_h - v))
                    dd_h[ii]
                })
                out <- list(wd_val = dd, ws_val = ff_h)
            }else{
                ff <- sapply(dd_h, function(v){
                    ii <- which.min(abs(ff_h - v))
                    ff_h[ii]
                })
                out <- list(wd_val = dd_h, ws_val = ff)
            }

            ## ZMD wind AWS
            frac <- out$ws_val %% 1
            out$wnd_hgt <- round(out$ws_val, 1)
            out$wnd_idx <- ((frac * 10) %% 1) * 10
            out <- as.data.frame(out)
            return(out)
        }

        return(NULL)
    })

    inull <- sapply(wndHgt, is.null)
    wndHgt <- wndHgt[!inull]
    wndHgt <- do.call(rbind, wndHgt)
    wndHgt <- wndHgt[!duplicated(wndHgt), , drop = FALSE]

    return(convJSON(wndHgt))
}

#############
#' Get AWS Status data.
#'
#' Get AWS Status data to display on map.
#' 
#' @param ltime character, the last time duration to display. Options are, "01h", "03h", "06h", 
#' "12h", "24h", "02d", "03d", "05d", "01w", "02w", "03w", "01m".
#' @param aws_dir full path to the directory containing ADT.\cr
#'               Example: "D:/ZMD_AWS_v2"
#' 
#' @return a JSON object
#' 
#' @export

readAWSStatus <- function(ltime, aws_dir){
    file_stat <- file.path(aws_dir, "AWS_DATA", "STATUS", "aws_status.rds")
    aws <- readRDS(file_stat)
    vtime <- as.numeric(substr(ltime, 1, 2))
    ttime <- substr(ltime, 3, 3)
    hmul <- switch(ttime, "h" = 1, "d" = 24, "w" = 168, "m" = 720)
    hour <- vtime * hmul
    if(hour > 1){
        ic <- (720 - hour + 1):720
        stat <- aws$status[, ic]
        stat <- rowMeans(stat, na.rm = TRUE)
    }else{
        nc <- ncol(aws$status)
        stat <- aws$status[, nc]
    }

    crds <- aws$coords
    crds$Availability <- paste(round(stat, 1), "%")
    kol <- cut(stat, c(0, 25, 50, 75, 100), labels = c('orange','yellow','green','blue'))
    kol <- as.character(kol)
    kol <- ifelse(is.na(kol), 'red', kol)
    crds$StatusX <- kol
    crds$longitude <- as.numeric(crds$longitude)
    crds$latitude <- as.numeric(crds$latitude)

    aws <- list(data = crds, time = aws$actual_time, update = aws$updated)
    return(convJSON(aws))
}
