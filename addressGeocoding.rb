require 'csv'
require 'net/http'
require 'json'
require 'parallel'

GOOGLE_API_KEY = 'AIzaSyCFemLGor-i0cjjQfjkndzOBuCiq7Btdz0'
ADDRESS_HEADERS = ['Nome', 'Endereco', 'Numero', 'CEP', 'Bairro', 'Cidade', 'Estado', 'Região', 'Pais']

def place_geocoding(place)
  uri = URI('https://maps.googleapis.com/maps/api/place/findplacefromtext/json')
  params = { query: place, key: GOOGLE_API_KEY }
  uri.query = URI.encode_www_form(params)

  res = Net::HTTP.get_response(uri)
  JSON.parse(res.body) if res.is_a?(Net::HTTPSuccess)
end

def address_geocoding(address)
  uri = URI('https://maps.googleapis.com/maps/api/geocode/json')
  params = { address: address, key: GOOGLE_API_KEY }
  uri.query = URI.encode_www_form(params)

  res = Net::HTTP.get_response(uri)
  JSON.parse(res.body) if res.is_a?(Net::HTTPSuccess)
end

def read_csv(file_name)
  CSV.read(file_name, headers: true)
end

def write_csv(data, new_columns, output)
  str = data.headers.concat(ADDRESS_HEADERS).join(",") + "\n"
  str += data.by_row.map.with_index do |r, i|
    r.fields.concat(new_columns[i]).map{|s| "\"" + s.to_s + "\""}.join(",")
  end.join("\n")
  File.write(output, str)
end

def map_collumn(types)
  if types.include?('street_number')
    "Numero"
  elsif types.include?('route')
    "Endereco"
  elsif types.include?('locality')
    "Cidade"
  elsif types.include?('sublocality')
    "Bairro"
  elsif types.include?('administrative_area_level_1')
    "Estado"
  elsif types.include?('administrative_area_level_2')
    "Cidade"
  elsif types.include?('country')
    "Pais"
  elsif types.include?('postal_code')
    "CEP"
  end
end

def split_address(address)
  result = Array.new(ADDRESS_HEADERS.size)
  json = address_geocoding(address)
  return result unless json && json['results'] && json['results'].any?
  json = address_geocoding(json['results'].first['formatted_address'])
  return result unless json && json['results'] && json['results'].any?

  json['results'].first['address_components'].each do |component|
    header_name = map_collumn(component['types'])
    next unless header_name
    i = ADDRESS_HEADERS.index(header_name)
    result[i] = component['long_name']
  end

  result
end

file_name = ARGV[0]
data = read_csv(file_name)
address_array = data.by_col!["Endereco"]
structured_address = Parallel.map(address_array) {|address| split_address(address)}

write_csv(data, structured_address, 'saida.csv')



