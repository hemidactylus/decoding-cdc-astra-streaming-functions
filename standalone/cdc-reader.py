import base64
import json
import io
import avro.schema
from avro.io import BinaryDecoder, DatumReader
#
import pulsar


service_url = INSERT_YOURS
token = INSERT_YOURS
topic_name = INSERT_YOURS

keySchemaDict = {
    "type": "record",
    "name": "reviews",
    "fields": [
        {
            "name": "hotel",
            "type": "string"
        },
        {
            "name": "id",
            "type": [
                "null",
                {
                    "type": "string",
                    "logicalType": "uuid"
                }
            ],
            "default": None
        }
    ]
}

valueSchemaDict = {
    "type": "record",
    "name": "reviews",
    "fields": [
        {
            "name": "body",
            "type": [
                "null",
                "string"
            ],
            "default": None
        },
        {
            "name": "reviewer",
            "type": [
                "null",
                "string"
            ],
            "default": None
        },
        {
            "name": "is_valid",
            "type": [
                "null",
                "boolean"
            ],
            "default": None
        },
        {
            "name": "score",
            "type": [
                "null",
                "int"
            ],
            "default": None
        }
    ]
}


def createAvroReader(schemaDict):
    return DatumReader(avro.schema.make_avsc_object(schemaDict))


def bytesToReadDict(by, avroReader):
    binDecoded = BinaryDecoder(io.BytesIO(by))
    return avroReader.read(binDecoded)


def b64ToReadDict(b64string, avroReader):
    b64Decoded = base64.b64decode(b64string)
    return bytesToReadDict(b64Decoded, avroReader)


def cdcMessageToDict(msg, keyReader, valueReader):
    return {
        **b64ToReadDict(msg.partition_key(), keyReader),
        **bytesToReadDict(msg.data(), valueReader),
    }


def tryReceive(consumer):
    try:
        msg = consumer.receive(5)
        return msg
    except Exception as e:
        if 'timeout' in str(e).lower():
            return None
        else:
            raise e


if __name__ == '__main__':
    # init to read schema
    keyReader = createAvroReader(keySchemaDict)
    valueReader = createAvroReader(valueSchemaDict)

    # client/consumer init
    client = pulsar.Client(service_url, authentication=pulsar.AuthenticationToken(token))
    consumer = client.subscribe(
        topic="persistent://" + topic_name,
        subscription_name='consumer1',
    )
    #
    while True:
        msg = tryReceive(consumer)
        if msg is not None:
            msgDict = cdcMessageToDict(msg, keyReader, valueReader)
            #
            print('\n\nRECEIVED:\n%s\n' % json.dumps(msgDict, indent=2))
            print('\n\nTYPES:\n%s\n' % json.dumps(
                {k: str(type(v)) for k, v in msgDict.items()},
                indent=2,
            ))
            consumer.acknowledge(msg)
