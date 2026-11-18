PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

SRCS(
    portion_info.proto
    index.proto
    essential_stats.proto
)

PEERDIR(
    ydb/library/formats/arrow/protos
    ydb/core/tx/columnshard/common/protos

)

END()
