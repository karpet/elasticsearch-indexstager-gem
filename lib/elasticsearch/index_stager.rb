require 'securerandom'

module Elasticsearch
  class IndexStager
    VERSION = '1.1.2'

    attr_reader :index_name, :es_client

    def initialize(opts)
      @index_name = opts[:index_name] or fail ":index_name required"
      @es_client = opts[:es_client] or fail ":es_client required"
    end

    def stage_index_name
      index_name + "_staged"
    end 

    def tmp_index_name
      @_suffix ||= Time.now.strftime('%Y%m%d%H%M%S') + '-' + SecureRandom.hex[0..7]
      "#{index_name}_#{@_suffix}"
    end 

    def alias_stage_to_tmp_index
      es_client.indices.delete index: stage_index_name rescue false
      es_client.indices.update_aliases body: {
        actions: [
          { add: { index: tmp_index_name, alias: stage_index_name } } 
        ]   
      }   
    end

    def promote(live_index_name=index_name)
      @live_index_name = live_index_name || index_name

      # the renaming actions (performed atomically by ES)
      rename_actions = [ 
        { remove: { index: stage_aliased_to, alias: stage_index_name } },
        {    add: { index: stage_aliased_to, alias: @live_index_name } } 
      ]   

      # zap any existing index known as index_name,
      # but do it conditionally since it is reasonable that it does not exist.
      to_delete = []
      live_index_exists = false
      begin
        existing_live_index = es_client.indices.get_alias(index: @live_index_name, name: '*')
        live_index_exists = true
      rescue Elasticsearch::Transport::Transport::Errors::NotFound => _err
        existing_live_index = {}
      rescue => _err
        raise _err
      end
      existing_live_index.each do |k,v|

        # if the index is merely aliased, remove its alias as part of the aliasing transaction.
        if k != @live_index_name
          rename_actions.unshift({ remove: { index: k, alias: @live_index_name } })

          # mark it for deletion when we've successfully updated aliases
          to_delete.push k

        else
          raise "Found existing index called #{@live_index_name} aliased to itself"
        end
      end

      if live_index_exists
        new_name = @live_index_name + '-pre-staged-original'

        # make a copy
        es_client.reindex body: { source: { index: @live_index_name }, dest: { index: new_name } }

        # make sure the copy exists before we delete the original
        tries = 0
        while( tries < 10 ) do
          indices = ESHelper.client.indices.get_aliases.keys
          break if indices.include?(new_name)
          tries += 1
          sleep(1)
        end

        # delete the original
        es_client.indices.delete index: @live_index_name rescue false
      end

      # re-alias
      es_client.indices.update_aliases body: { actions: rename_actions }

      # clean up
      to_delete.each do |idxname|
        es_client.indices.delete index: idxname rescue false
      end
    end

    private

    def tmp_index_pattern
      /#{index_name}_(\d{14})-\w{8}$/
    end

    def stage_aliased_to
      # find the newest tmp index to which staged is aliased.
      # we need this because we want to re-alias it.
      aliased_to = find_newest_alias_for(stage_index_name)
    end

    def find_newest_alias_for(the_index_name)
      aliased_to = nil
      aliases = es_client.indices.get_alias(index: the_index_name, name: '*')
      aliases.each do |k,v|
        next unless k.match(tmp_index_pattern)
        aliased_to ||= k
        alias_tstamp = aliased_to.match(tmp_index_pattern)[1]
        k_tstamp = k.match(tmp_index_pattern)[1]
        if Time.parse(alias_tstamp) < Time.parse(k_tstamp)
          aliased_to = k
        end
      end
      if !aliased_to
        raise "Cannot identify index aliased to by '#{the_index_name}'"
      end
      aliased_to
    end

  end
end
