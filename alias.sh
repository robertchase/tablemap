_BIN=$(make bin)

alias run="$_BIN/python3"
alias test="$_BIN/pytest"

unset _BIN

alias un-alias="unalias un-alias run test"
