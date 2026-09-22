       >>SOURCE FORMAT FREE
identification division.
program-id. ReferenceDriver.
environment division.
input-output section.
file-control.
    select account-index assign to ACCTFILE
      organization indexed access dynamic record key account-key
      file status fs.
    select xref-index assign to XREFFILE
      organization indexed access dynamic record key xref-key
      alternate record key xref-account file status fs.
    select category-index assign to TCATBALF
      organization indexed access dynamic record key category-key
      file status fs.
    select rate-index assign to DISCGRP
      organization indexed access dynamic record key rate-key
      file status fs.
    select transaction-output assign to TRANSACT
      organization sequential access sequential file status fs.
    select input-lines assign to dynamic input-name
      organization line sequential file status input-fs.
    select output-lines assign to dynamic output-name
      organization line sequential file status output-fs.
data division.
file section.
fd account-index.
01 account-data.
   05 account-key pic 9(11).
   05 filler pic x(289).
fd xref-index.
01 xref-data.
   05 xref-key pic x(16).
   05 filler pic 9(9).
   05 xref-account pic 9(11).
   05 filler pic x(14).
fd category-index.
01 category-data.
   05 category-key pic x(17).
   05 filler pic x(33).
fd rate-index.
01 rate-data.
   05 rate-key pic x(16).
   05 filler pic x(34).
fd transaction-output.
01 transaction-data pic x(350).
fd input-lines.
01 input-line pic x(1024).
fd output-lines.
01 output-line pic x(1024).
working-storage section.
       >>SOURCE FORMAT FIXED
       copy 'CVACT01Y.cpy'.
       copy 'CVACT03Y.cpy'.
       copy 'CVTRA01Y.cpy'.
       copy 'CVTRA02Y.cpy'.
       copy 'CVTRA05Y.cpy'.
       >>SOURCE FORMAT FREE
01 fs pic xx.
01 input-fs pic xx.
01 output-fs pic xx.
01 input-name pic x(1024).
01 output-name pic x(1024).
01 action-name pic x(20).
01 eof-flag pic 9.
01 fields.
   05 field-value pic x(100) occurs 12.
01 amount-one pic +9(10).99.
01 amount-two pic +9(10).99.
01 amount-three pic +9(10).99.
01 amount-four pic +9(10).99.
01 amount-five pic +9(10).99.
01 transaction-amount pic +9(9).99.
01 external-parms.
   05 parm-length pic s9(4) comp value 10.
   05 parm-date pic x(10).
procedure division.
    accept action-name from command-line
    evaluate function trim(action-name)
      when 'seed'
        perform seed-accounts
        perform seed-xrefs
        perform seed-categories
        perform seed-rates
      when 'run'
        accept parm-date from environment 'REF_BATCH_DATE'
        call 'CBACT04C' using external-parms
      when 'export'
        perform export-accounts
        perform export-transactions
      when other
        display 'Expected seed, run or export'
        move 2 to return-code
        stop run
    end-evaluate
    move 0 to return-code
    stop run.
check-file.
    if fs not = '00'
      display 'HARNESS FILE ERROR: ' fs
      move 2 to return-code
      stop run
    end-if.
read-line.
    move spaces to input-line fields
    read input-lines
      at end move 1 to eof-flag
      not at end
        unstring input-line delimited by '|'
          into field-value(1) field-value(2) field-value(3)
               field-value(4) field-value(5) field-value(6)
               field-value(7) field-value(8) field-value(9)
               field-value(10) field-value(11) field-value(12)
        end-unstring
    end-read
    if input-fs not = '00' and input-fs not = '10'
      display 'HARNESS INPUT ERROR: ' input-fs
      move 2 to return-code
      stop run
    end-if.
open-input.
    open input input-lines
    if input-fs not = '00'
      display 'HARNESS INPUT OPEN ERROR: ' input-fs
      move 2 to return-code
      stop run
    end-if
    move 0 to eof-flag.
write-output.
    write output-line
    if output-fs not = '00'
      display 'HARNESS EXPORT ERROR: ' output-fs
      move 2 to return-code
      stop run
    end-if.
seed-accounts.
    accept input-name from environment 'REF_ACCOUNTS_INPUT'
    perform open-input
    open output account-index
    perform check-file
    perform until eof-flag = 1
      perform read-line
      if eof-flag = 0
        initialize account-record
        move function numval(field-value(1)) to acct-id
        move field-value(2) to acct-group-id
        move function numval(field-value(3)) to acct-curr-bal
        move function numval(field-value(4)) to acct-curr-cyc-credit
        move function numval(field-value(5)) to acct-curr-cyc-debit
        move field-value(6) to acct-active-status
        move 10000 to acct-credit-limit
        move 5000 to acct-cash-credit-limit
        move '2020-01-01' to acct-open-date
        move '2030-01-01' to acct-expiraion-date
        move '2025-01-01' to acct-reissue-date
        move '12345' to acct-addr-zip
        write account-data from account-record
        perform check-file
      end-if
    end-perform
    close input-lines account-index.
seed-xrefs.
    accept input-name from environment 'REF_XREFS_INPUT'
    perform open-input
    open output xref-index
    perform check-file
    perform until eof-flag = 1
      perform read-line
      if eof-flag = 0
        initialize card-xref-record
        move field-value(1) to xref-card-num
        move function numval(field-value(2)) to xref-cust-id
        move function numval(field-value(3)) to xref-acct-id
        write xref-data from card-xref-record
        perform check-file
      end-if
    end-perform
    close input-lines xref-index.
seed-categories.
    accept input-name from environment 'REF_CATEGORIES_INPUT'
    perform open-input
    open output category-index
    perform check-file
    perform until eof-flag = 1
      perform read-line
      if eof-flag = 0
        initialize tran-cat-bal-record
        move function numval(field-value(1)) to trancat-acct-id
        move field-value(2) to trancat-type-cd
        move function numval(field-value(3)) to trancat-cd
        move function numval(field-value(4)) to tran-cat-bal
        write category-data from tran-cat-bal-record
        perform check-file
      end-if
    end-perform
    close input-lines category-index.
seed-rates.
    accept input-name from environment 'REF_RATES_INPUT'
    perform open-input
    open output rate-index
    perform check-file
    perform until eof-flag = 1
      perform read-line
      if eof-flag = 0
        initialize dis-group-record
        move field-value(1) to dis-acct-group-id
        move field-value(2) to dis-tran-type-cd
        move function numval(field-value(3)) to dis-tran-cat-cd
        move function numval(field-value(4)) to dis-int-rate
        write rate-data from dis-group-record
        perform check-file
      end-if
    end-perform
    close input-lines rate-index.
export-accounts.
    accept output-name from environment 'REF_ACCOUNTS_OUTPUT'
    open output output-lines
    open input account-index
    perform check-file
    perform until fs = '10'
      read account-index next record into account-record
      if fs = '00'
        move acct-curr-bal to amount-one
        move acct-curr-cyc-credit to amount-two
        move acct-curr-cyc-debit to amount-three
        move acct-credit-limit to amount-four
        move acct-cash-credit-limit to amount-five
        move spaces to output-line
        string acct-id '|' function trim(acct-group-id) '|'
          amount-one '|' amount-two '|' amount-three '|'
          acct-active-status '|' amount-four '|' amount-five '|'
          acct-open-date '|' acct-expiraion-date '|'
          acct-reissue-date '|' function trim(acct-addr-zip)
          delimited by size into output-line
        end-string
        perform write-output
      else
        if fs not = '10' perform check-file end-if
      end-if
    end-perform
    close account-index output-lines.
export-transactions.
    accept output-name from environment 'REF_TRANSACTIONS_OUTPUT'
    open output output-lines
    open input transaction-output
    perform check-file
    perform until fs = '10'
      read transaction-output into tran-record
      if fs = '00'
        *> STRING in the original leaves its description padding untouched.
        *> Normalize runtime LOW-VALUES padding only in this text export.
        inspect tran-desc replacing all low-values by spaces
        move tran-amt to transaction-amount
        move spaces to output-line
        string tran-id '|' tran-type-cd '|' tran-cat-cd '|'
          function trim(tran-source) '|'
          function trim(tran-desc) '|' transaction-amount '|'
          tran-card-num '|' tran-orig-ts '|' tran-proc-ts '|'
          tran-merchant-id '|'
          function trim(tran-merchant-name) '|'
          function trim(tran-merchant-city) '|'
          function trim(tran-merchant-zip)
          delimited by size into output-line
        end-string
        perform write-output
      else
        if fs not = '10' perform check-file end-if
      end-if
    end-perform
    close transaction-output output-lines.
end program ReferenceDriver.
