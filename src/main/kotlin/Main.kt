package za.proto.mock

import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import com.google.protobuf.DynamicMessage
import com.google.protobuf.TextFormat
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import java.io.File
import java.util.*

fun main() {
    val args = arrayListOf(
        "-m", "bidmachine.protobuf.openrtb.Openrtb",
        "-f", "bidmachine.protobuf.openrtb.Openrtb.json",
        "-d", "bidmachine.desc", "google.desc"
    )

    args.fold(Pair(emptyMap<String, List<String>>(), "")) { (map, lastKey), elem ->
        if (elem.startsWith("-")) {
            Pair(map + (elem to emptyList()), elem)
        } else {
            Pair(map + (lastKey to map.getOrDefault(lastKey, emptyList()) + elem), lastKey)
        }
    }.first.let { argsMap ->
        startMockServer(argsMap["-m"], argsMap["-f"], argsMap["-d"], argsMap["-p"])
    }
}

private fun printHelp() {
    println("Usage: -p <port> (optional, default 8080) -m <Protobuf message full type name> -f <file> -d <Descriptor file 1> .. <Descriptor file N>")
}

private fun startMockServer(messageTypeNames: List<String>?,
                            files: List<String>?,
                            descriptorFiles: List<String>?,
                            ports: List<String>?) {
    val messageTypeName = messageTypeNames?.first()
    val file = files?.first()
    if (messageTypeName == null || file == null || descriptorFiles.isNullOrEmpty()) {
        printHelp()
        return
    }
    val text = File(file).readText()
    if (text.isEmpty()) {
        println("File $file not exists or empty!")
        return
    }

    descriptorFiles.map { descriptorFile ->
        val descriptorFileBytes = object {}.javaClass.getResource("/$descriptorFile")?.readBytes()
        DescriptorProtos.FileDescriptorSet.parseFrom(descriptorFileBytes)
    }.let { fileDescSets ->
        getFileDescriptorProtos(fileDescSets)
    }.let { map ->
        createMessageDescriptor(messageTypeName, map, null)
    }.let { descriptor ->
        startServer(text, descriptor, ports)
    }
}

private fun createResponse(text: String, descriptor: Descriptors.Descriptor?): DynamicMessage {
    return DynamicMessage.newBuilder(descriptor).let { dynamicMessageBuilder ->
        processResponseTemplate(text).let { result ->
            TextFormat.getParser().merge(result, dynamicMessageBuilder)
        }
        dynamicMessageBuilder.build()
    }
}

private fun processResponseTemplate(template: String): String {
    return template.replace("<BID_ID>", UUID.randomUUID().toString())
}

private fun startServer(text: String, descriptor: Descriptors.Descriptor?, ports: List<String>?) {
    val environment = applicationEngineEnvironment {
        connector {
            host = "127.0.0.1"
            port = ports?.first()?.toInt() ?: 8080
        }
        module {
            routing {
                post("/") {
                    val response = createResponse(text, descriptor)
                    call.respondBytes(response.toByteArray(),
                                      ContentType("application",
                                                  "x-protobuf; messageType=\"${response.descriptorForType.fullName}\"",
                                                  emptyList()),
                                      HttpStatusCode.OK)
                }
            }
        }
    }
    embeddedServer(Netty, environment).start(wait = true)
}

private fun getFileDescriptorProtos(fileDescSets: List<DescriptorProtos.FileDescriptorSet>) =
    fileDescSets.flatMap { fileDescSet ->
        fileDescSet.fileList.map { fileDescProto -> fileDescProto }
    }.map { fileDescProto ->
        getProtoFileName(fileDescProto.name) to fileDescProto
    }.toMap()

private fun getProtoFileName(fullPath: String): String = fullPath.split("/").last()

private fun createMessageDescriptor(messageTypeName: String,
                            map: Map<String, DescriptorProtos.FileDescriptorProto>,
                            parentMessageDescriptor: Descriptors.Descriptor? = null): Descriptors.Descriptor? {
    val fileDescriptorProto = getProtoFileForMessage(messageTypeName, map)
        ?: throw Exception("Message $messageTypeName not found!")

    println("Proto file ${fileDescriptorProto.name} contains message type $messageTypeName")

    return getDependencies(map, fileDescriptorProto).let { dependencies ->
        Descriptors.FileDescriptor.buildFrom(fileDescriptorProto, dependencies)
    }.findMessageTypeByNameOrFullName(messageTypeName)
        ?: findMessageDescriptorInHierarchy(messageTypeName, parentMessageDescriptor)
}

private fun getProtoFileForMessage(messageTypeName: String,
                           map: Map<String, DescriptorProtos.FileDescriptorProto>): DescriptorProtos.FileDescriptorProto? =
    map.values.firstOrNull { fileDescriptorProto ->
        messageInList(fileDescriptorProto.messageTypeList, fileDescriptorProto.`package`, messageTypeName)
    }

private fun messageInList(messageTypeList: List<DescriptorProtos.DescriptorProto>,
                  packageName: String,
                  messageTypeName: String): Boolean {
    return messageTypeList.any {
        messageTypeName == it.name
                || messageTypeName == "$packageName.${it.name}"
                || messageInList(it.nestedTypeList, packageName, messageTypeName)
    }
}

private fun getDependencies(fileDescProtoMap: Map<String, DescriptorProtos.FileDescriptorProto>,
                    fileDescProto: DescriptorProtos.FileDescriptorProto): Array<Descriptors.FileDescriptor> =
    fileDescProto.dependencyList.filter {
        fileDescProtoMap.containsKey(getProtoFileName(it))
    }.map {
        Descriptors.FileDescriptor.buildFrom(fileDescProtoMap[getProtoFileName(it)],
                                             getDependencies(fileDescProtoMap,
                                                             fileDescProtoMap[getProtoFileName(it)]!!))
    }.toTypedArray()

private fun findMessageDescriptorInHierarchy(messageTypeName: String,
                                     parent: Descriptors.Descriptor?): Descriptors.Descriptor? =
    parent?.findNestedTypeByName(messageTypeName)
        ?: findMessageDescriptorInHierarchy(messageTypeName, parent?.containingType)

// EXTENSIONS

private fun Descriptors.FileDescriptor.findMessageTypeByNameOrFullName(name: String): Descriptors.Descriptor? {
    return this.findMessageTypeByName(name)
        ?: this.findMessageTypeByName(name.replace("${this.`package`}.", ""))
}