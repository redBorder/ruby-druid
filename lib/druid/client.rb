#
# Ruby Druid module
#
module Druid
  #
  # Manage client connection and queries
  #
  class Client
    #
    # Init a client to send queries to Druid.
    #
    # == Parameters:
    # zookeeper_uri::
    #   An String with the URI to connecto to zookeper
    # opts::
    #   Hash with options. Permitted options:
    #     - static_setup: String with literal broker URL (not use zookeper)
    #     - fallback: Boolean. True to use static_setup as backup
    #     - http_timeout: integer of seconds to define a timeout to queries
    #
    def initialize(zookeeper_uri, opts = nil)
      opts ||= {}

      # Set options
      if opts[:static_setup] && !opts[:fallback]
        @static = opts[:static_setup]
      else
        @backup = opts[:static_setup] if opts[:fallback]
        # Init ZK
        @zk = ZooHandler.new(zookeeper_uri, opts)
      end

      @http_timeout = opts[:http_timeout] || 2 * 60
    end

    #
    # Send a query object to Druid based on currenc conection.
    #
    # == Parameters:
    # query::
    #   Query object
    #
    # == Returns:
    # Parsed JSON with the response data.
    #
    def send(query)
      # Get the response
      response = http_query(broker_uri, query)

      if response.code == '200'
        JSON.parse(response.body).map { |row| ResponseRow.new(row) }
      else
        fail "Request failed: #{response.code}: #{response.body}"
      end
    end

    #
    # Perform the HTTP request to druid.
    #
    # == Parameters:
    # uri::
    #   URI object to send the query
    # query::
    #   Query object
    #
    # == Returns:
    # HTTP response
    #
    def http_query(uri, query)
      # Set headers
      req = Net::HTTP::Post.new(uri.path, 'Content-Type' => 'application/json')
      req.body = query.to_json

      # Perform the query
      Net::HTTP.new(uri.host, uri.port).start do |http|
        http.read_timeout = @http_timeout
        http.request(req)
      end
    end

    #
    # Create a query Object. If a block given, launch the query.
    #
    # == Parameters:
    # id::
    #   String or Array of strings with the data source
    # block::
    #   Ruby block to execute after send the query
    #
    # == Returns:
    # Query Object or execute ruby block
    #
    def query(id, &block)
      query = Query.new(id, self)
      return query unless block
      # Send the query if a blocks given
      send query
    end

    #
    # Return the URI of an available broker
    #
    # == Returns:
    # URI object
    #
    def broker_uri
      # Get a random broker to perform the query
      uri = @static || @zk.broker
      # Try to initiate or get backup.
      begin
        uri = URI(uri) if uri
      rescue
        uri = URI(@backup) if @backup
      end
      # Fail or return uri
      fail "Broker #{uri} (currently) not available" unless uri
      uri
    end

    #
    # Get the info for a DataSource
    #
    def data_source(source)
      uri = @static || @zk.broker
      fail "DataSource #{source} (currently) not available" unless uri

      # Path to get the info
      meta_path = "#{uri.path}datasources/#{source}"

      req = Net::HTTP::Get.new(meta_path)
      response = Net::HTTP.new(uri.host, uri.port).start do |http|
        http.read_timeout = @http_timeout
        http.request(req)
      end

      if response.code == '200'
        meta = JSON.parse(response.body)
        meta.define_singleton_method(:dimensions) { self['dimensions'] }
        meta.define_singleton_method(:metrics) { self['metrics'] }
        meta
      else
        fail 'Request failed for dataSource '\
             "#{source} - #{response.code}: #{response.body}"
      end
    end
  end
end
