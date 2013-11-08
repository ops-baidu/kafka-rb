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
#zookeeper consistent-hashing feature,added by liyong
    def get_host_from_zk(zkhost, zkport)
      require 'zookeeper'
      require 'consistent_hashing'
      require 'socket'
      @z = Zookeeper.new("#{zkhost}:#{zkport}")
      @ring = ConsistentHashing::Ring.new
      @host_local = Socket.gethostname
      brokers = @z.get_children(:path => "/brokers/ids")[:children]
      brokers.each do |broker|
        res = @z.get(:path => "/brokers/ids/#{broker}")[:data]
        @ring << res
      end
      @z.close
      @ring.node_for(@host_local).split(":")[1..-1]
    end
    def zkconnect(zkhost, zkport)
      raise ArgumentError, "No zkhost or zkport specified" unless zkhost && zkport
      self.zkhost = zkhost
      self.zkport = zkport
      self.host, self.port = get_host_from_zk(self.zkhost, self.zkport)
      self.socket = TCPSocket.new(self.host, self.port)
    end
########################################################

    def reconnect
      self.socket = TCPSocket.new(self.host, self.port)
    rescue
      self.disconnect
      raise
    end

#zookeeper consistent-hashing feature,added by liyong
    def zkreconnect
      self.host, self.port = get_host_from_zk(self.zkhost, self.zkport)
      self.socket = TCPSocket.new(self.host, self.port)
    rescue
      self.disconnect
      raise
    end
#####################################################

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

#zookeeper consistent-hashing feature,added by liyong
    def zkwrite(data)
      self.zkreconnect unless self.socket
      self.socket.write(data)
    rescue
      self.disconnect
      raise SocketError, "cannot write: #{$!.message}"
    end
#####################################################

  end
end
