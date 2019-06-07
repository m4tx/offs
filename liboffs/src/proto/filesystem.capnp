@0x97caa69584521eb1;

using FileMode = UInt16;
using FileDev = UInt32;

interface RemoteFsProto {
    # Read
    list @0 (id :Text) -> (list :List(DirEntity));
    listChunks @1 (id :Text) -> (list :List(Text));
    getBlobs @2 (ids :List(Text)) -> (list :List(Blob));

    # Modify ops
    applyOperation @3 (operation :ModifyOperation) -> (dirEntity :DirEntity);
    applyJournal @4 (
        list :List(ModifyOperation),
        chunks :List(List(Text)),
        blobs :List(Data)
    ) -> (
        error :Error,
        assignedIds :List(Text),
        dirEntities :List(DirEntity)
    );
    getMissingBlobs @5 (ids :List(Text)) -> (list :List(Text));
}

struct Error {
    union {
        none @0 :Void;

        invalidJournal @1 :Void;
        conflictingFiles :group {
            ids @2 :List(Text);
        }
        missingBlobs :group {
            ids @3 :List(Text);
        }
    }
}

struct Timespec {
    sec @0 :Int64;
    nsec @1 :Int32;
}

struct StatDelta {
    const nullMode :UInt16 = 0xFFFF;
    const nullUid :UInt32 = 0xFFFFFFFF;
    const nullGid :UInt32 = 0xFFFFFFFF;
    const nullSize :UInt64 = 0xFFFFFFFFFFFFFFFF;

    mode @0 :FileMode = StatDelta.nullMode;
    uid @1 :UInt32 = StatDelta.nullUid;
    gid @2 :UInt32 = StatDelta.nullGid;
    size @3 :UInt64 = StatDelta.nullSize;
    atim @4 :Timespec;
    mtim @5 :Timespec;
}

enum FileType {
    namedPipe @0;
    charDevice @1;
    blockDevice @2;
    directory @3;
    regularFile @4;
    symlink @5;
    socket @6;
}

struct Stat {
    ino @0 :UInt64; # Inode number
    fileType @1 :FileType; # File type
    mode @2 :FileMode; # File permissions
    dev @3 :FileDev; # Device
    nlink @4 :UInt64; # Number of hard links
    uid @5 :UInt32; # User ID of owner
    gid @6 :UInt32; # Group ID of owner
    size @7 :UInt64; # Total size, in bytes
    blocks @8 :UInt64; # Number of 512B blocks allocated
    atim @9 :Timespec; # Time of last access
    mtim @10 :Timespec; # Time of last modification
    ctim @11 :Timespec; # Time of last status change
}

struct DirEntity {
    id @0 :Text;
    parent @1 :Text;
    name @2 :Text;

    direntVersion @3 :Int64;
    contentVersion @4 :Int64;

    stat @5 :Stat;
}

struct Blob {
    id @0 :Text;
    content @1 :Data;
}

struct ModifyOperation {
    id @0 :Text;
    # ID of the file the operation is made on. In case of create(File|Directory)
    # this is the ID of the parent directory of the newly created entity

    timestamp @1 :Timespec;

    direntVersion @2 :Int64;
    contentVersion @3 :Int64;

    union {
        # Create
        createFile :group {
            name @4 :Text;
            fileType @5 :FileType;
            mode @6 :FileMode;
            dev @7 :FileDev;
        }
        createSymlink :group {
            name @8 :Text;
            link @9 :Text;
        }
        createDirectory :group {
            name @10 :Text;
            mode @11 :FileMode;
        }

        # Remove
        removeFile :group {
            dummy @12 :Void;
        }
        removeDirectory :group {
            dummy @13 :Void;
        }

        # Modify
        rename :group {
            newParent @14 :Text;
            newName @15 :Text;
        }
        setAttributes :group {
            toSave @16 :StatDelta;
        }
        write :group {
            offset @17 :Int64;
            data @18 :Data;
        }
    }
}
