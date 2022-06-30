'''
Created on Jul 7, 2022

@author: ritesh.agarwal
'''
from java.nio.file import Paths
from com.spotify.docker.client.messages import ContainerConfig, HostConfig,\
    PortBinding
from com.spotify.docker.client import DefaultDockerClient
from Cb_constants.CBServer import CbServer


class DockerClient:

    def __init__(self):
        self.docker = DefaultDockerClient.fromEnv().build()

    def buildImage(self, dockerFilePath, imageName):
        docker_image_id = self.docker.build(Paths.get(dockerFilePath), imageName)
        return docker_image_id

    def portMapping(self, portMap={}):
        # Bind container ports to host ports
        default_map = {str(CbServer.port): str(CbServer.port),
                       str(CbServer.memcached_port): str(CbServer.memcached_port)}
        portMap.update(default_map)
        portBindings = {}
        for hostPort in portMap.keys():
            containerPort = portMap[hostPort]
            hostPorts = []
            hostPorts.append(PortBinding.of("0.0.0.0", hostPort))
            portBindings.update({containerPort: hostPorts})
            portMap[containerPort] = hostPort
        hostConfig = HostConfig.builder().portBindings(portBindings).build()
        return hostConfig

    def startDockerContainer(self, hostConfig, docker_image_id,
                             exposedPorts=None, tag="taf_container"):
        exposedPorts = exposedPorts or [str(CbServer.port), str(CbServer.memcached_port)]
        config = ContainerConfig\
            .builder()\
            .hostConfig(hostConfig)\
            .exposedPorts(set(exposedPorts))\
            .image(docker_image_id).build()
        creation = self.docker.createContainer(config, tag)
        container_id = creation.id()
        self.docker.startContainer(container_id)
        return container_id

    def inspectContainer(self, containerId):
        return self.docker.inspectContainer(containerId)

    def containerLogs(self, containerId):
        command = {"sh", "-c", "ls"}
        execCreation = self.docker.execCreate(
            containerId, command, DockerClient.ExecCreateParam.attachStdout(),
            DockerClient.ExecCreateParam.attachStderr())
        output = self.docker.execStart(execCreation.id())
        execOutput = output.readFully()
        return execOutput

    def killContainer(self, containerId):
        self.docker.killContainer(containerId)
        self.docker.removeContainer(containerId)

    def deleteImage(self, imageID):
        self.docker.removeImage(imageID)

    def close(self):
        self.docker.close()
