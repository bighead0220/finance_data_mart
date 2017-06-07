# filename: strip_sql_comment.awk  
# issue: awk -f scrip_sql_comment.awk test.sql  
BEGIN { FS="" }
!(ignore_line && $NF == "\\") && !ignore_line-- {  
    ignore_line = 0;  
    for(i = 1; i <= NF; i++) {  
        if (ignore_block) {  
            if ($i $(i+1) == "*/") {  
                ignore_block = 0  
                i++ # remove '*' 
            }  
            continue 
        }  
        if (!instr && $i $(i+1) == "/*") {  
            ignore_block = 1  
            i++ # remove '/' 
            continue 
        }
        if (!instr && $i $(i+1) == "--") {  
            ignore_line = ($NF == "\\")? 1: 0  
            break  
        }  
        if ($i == "\"") {  
            instr = 1 - instr  
        }  
        printf($i)  
    }  
    printf("\n")  
}
