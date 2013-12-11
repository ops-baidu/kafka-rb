# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
module Kafka
  module IO
    attr_accessor :socket, :host, :port, :compression, :zkhost, :zkport

#    HOST = "localhost"
#    PORT = 9092

    def connect(host, port)
      raise ArgumentError, "No host or port specified" unless host && port
      self.host = host
      self.port = port
      self.socket = TCPSocket.new(host, port)
    end

    def get_kafka_host_from_zk(zkhost, zkport)
      require 'zookeeper'

      zk = Zookeeper.new("#{zkhost}:#{zkport}")
      brokers = zk.get_children(:path => "/brokers/ids")[:children]
      host_port = zk.get(:path => "/brokers/ids/#{brokers.min}")[:data]
      zk.close

      host_port.split(":")[-2..-1]
    end

    def zkconnect(zkhost, zkport)
      raise ArgumentError, "No zkhost or zkport specified" unless zkhost && zkport
      self.zkhost = zkhost
      self.zkport = zkport
      self.host, self.port = get_kafka_host_from_zk(self.zkhost, self.zkport)
      connect(self.host, self.port)
    end

    def reconnect
      if self.zkhost and self.zkport
        self.host, self.port = get_kafka_host_from_zk(self.zkhost, self.zkport)
      end
      self.socket = TCPSocket.new(self.host, self.port)
    rescue
      self.disconnect
      raise
    end

    def disconnect
      self.socket.close rescue nil
      self.socket = nil
    end

    def read(length)
      self.socket.read(length) || raise(SocketError, "no data")
    rescue
      self.disconnect
      raise SocketError, "cannot read: #{$!.message}"
    end

    def write(data)
      self.reconnect unless self.socket
      self.socket.write(data)
    rescue
      self.disconnect
      raise SocketError, "cannot write: #{$!.message}"
    end

  end
end
