require 'zk'
require 'json'
require 'rest_client'

module Druid
  #
  # Class to connect Zookeeper and fetch all available brokers to perform
  # queries.
  #
  class ZooHandler
    # Broker service name
    BROKER_SERVICE = 'broker'
    VERSION = 'v2'

    def initialize(uri, opts = {})
      # Initialize vars
      @zk = ZK.new uri, chroot: :check
      @discovery_path = opts[:discovery_path] || '/discoveryPath'
      # Array of brokers
      @brokers = []
      # Watch brokers
      @watcher = {}

      init_zookeeper
    end

    #
    # Connect to zookeeper and check if broker service is available.
    # It adds a listener to reload services when zookeeper changes.
    #
    def init_zookeeper
      # Init when expired zookeeper session
      @zk.on_expired_session do
        init_zookeeper
      end

      # Add a listener for discovery path on zookeeper
      @zk.register(@discovery_path, only: :child) do |_|
        check_brokers
      end

      # Load brokers
      check_brokers
    end

    def close!
      @zk.close!
    end

    #
    # Return the URI of a random available broker.
    # Poor mans load balancing
    # 
    def broker
      return nil if @brokers.size == 0
      # Return a random broker from available brokers
      i = Random.rand(@brokers.size)
      @brokers[i][:uri]
    end

    #
    # Check available brokers
    #
    def check_brokers
      # Get all services
      zk_services = @zk.children @discovery_path, watch: true

      # Try to select broker or fail
      if zk_services.include? BROKER_SERVICE
        load_brokers
      else
        # There aren't any broker defined on the node!
        fail 'There aren\'t any broker defined!'
      end
    end

    #
    # Load brokers from ZK and store them at @brokers
    #
    def load_brokers
      return unless @watcher.empty?

      # Add a watcher to reload brokers.
      watch_path = "#{@discovery_path}/#{BROKER_SERVICE}"

      @watcher =
        @zk.register(watch_path, only: :child) do |_|
          @watcher.unregister if @watcher
          @watcher = nil
          # Reload brokers
          check_brokers
        end

      # Get @brokers cached and brokers from ZK
      known = @brokers.map { |node| node[:name] } rescue []
      live = @zk.children(watch_path, watch: true)

      # copy the unchanged entries
      new_list = @brokers.select { |node| live.include? node[:name] } rescue []

      # verify the new entries to be living brokers
      (live - known).each do |name|
        # Get the URI
        info = @zk.get "#{watch_path}/#{name}"
        node = JSON.parse(info[0])
        uri =  "http://#{node['address']}:#{node['port']}/druid/#{VERSION}/"
            
        new_list.push(
          name: name,
          uri: uri
        )
      end

      @brokers = new_list
    end

    #
    # Only return current brokers.
    #
    def to_s
      @brokers.to_s
    end
  end
end
