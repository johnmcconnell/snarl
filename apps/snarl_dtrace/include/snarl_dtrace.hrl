-define(DT_SNARL, 4201).
-define(DT_SNARL_READ, 4202).
-define(DT_SYNC_SEND, 4203).

-define(DT_ENTRY, 1).
-define(DT_RETURN, 2).

-define(DT_FOUND, 1).
-define(DT_NOTFOUND, 2).
-define(DT_OK, 1).
-define(DT_FAIL, 2).

-define(DT_GENERIC, 0).
-define(DT_READ, 1).
-define(DT_SYNC, 3).


-define(DT_READ_ENTRY(Key, Op),
        dyntrace:p(?DT_SNARL_READ, ?DT_ENTRY, atom_to_list(?MODULE), Key, atom_to_list(Op))).

-define(DT_READ_FOUND_RETURN(Key, Op),
        dyntrace:p(?DT_SNARL_READ, ?DT_RETURN, ?DT_FOUND, atom_to_list(?MODULE), Key, atom_to_list(Op))).

-define(DT_READ_NOT_FOUND_RETURN(Key, Op),
        dyntrace:p(?DT_SNARL_READ, ?DT_RETURN, ?DT_NOTFOUND, atom_to_list(?MODULE), Key, atom_to_list(Op))).

-define(DT_ENTRY(Name), dyntrace:p(?DT_SNARL, ?DT_ENTRY, atom_to_list(?MODULE), Name)).

-define(DT_RETURN(Name), dyntrace:p(?DT_SNARL, ?DT_ENTRY, atom_to_list(?MODULE), Name)).

