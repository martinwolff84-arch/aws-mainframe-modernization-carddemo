       >>SOURCE FORMAT FREE
identification division.
program-id. CEE3ABD.
data division.
linkage section.
01 abend-code pic s9(9) binary.
01 cleanup-mode pic s9(9) binary.
procedure division using abend-code cleanup-mode.
    display 'REFERENCE ABEND STUB: ' abend-code
    move 99 to return-code
    stop run.
end program CEE3ABD.
