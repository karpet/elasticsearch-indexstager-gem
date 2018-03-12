require 'spec_helper'
require 'pp'

describe Elasticsearch::IndexStager do

  def delete_all_indices
    indices = ESHelper.client.indices.get_aliases
    pp indices
    ESHelper.client.indices.delete(index: indices.keys) if indices.keys.any?
  end

  before(:each) do
    delete_all_indices
  end

  after(:each) do
    delete_all_indices
  end

  it "generates index names" do
    stager = Elasticsearch::IndexStager.new(index_name: 'articles', es_client: ESHelper.client)
    expect(stager.stage_index_name).to eq "articles_staged"
    expect(stager.tmp_index_name).to match(/^articles_\d{14}-\w{8}$/)
  end

  it "stages an index" do
    stager = stage_index
    aliases = ESHelper.client.indices.get_alias(index: stager.stage_index_name)
    expect(aliases.keys.size).to eq 1
    expect(aliases.keys[0]).to eq stager.tmp_index_name
  end

  it "promotes a staged index to live" do
    stager = stage_index
    stager.promote
    ESHelper.refresh(stager.index_name)

    response = ESHelper.client.search(index: stager.index_name, body: { query: { match: { title: 'test' } } } )
    expect(response['hits']['total']).to eq 2

    aliases = ESHelper.client.indices.get_alias(index: stager.index_name)
    expect(aliases.keys[0]).to eq stager.tmp_index_name
  end

  it "handles first-time migration to staged paradigm" do
    create_index('articles')
    stager = stage_index
    stager.promote
    ESHelper.refresh(stager.index_name)

    aliases = ESHelper.client.indices.get_alias(index: stager.index_name, name: '*')
    expect(aliases.keys[0]).to eq stager.tmp_index_name

    # the original was saved
    orig_name = stager.index_name + '-pre-staged-original'
    expect(ESHelper.client.indices.get_aliases.keys).to include(orig_name)
  end

  def create_index(index_name)
    ESHelper.client.index(index: index_name, type: 'article', id: 1, body: { title: 'Test' })
    ESHelper.client.index(index: index_name, type: 'article', id: 2, body: { title: 'Test' })
  end

  def stage_index
    stager = Elasticsearch::IndexStager.new(index_name: 'articles', es_client: ESHelper.client)
    create_index(stager.tmp_index_name)
    stager.alias_stage_to_tmp_index
    stager
  end
end
