box.cfg()

local vk = box.schema.space.create('vk', {
    format = {
        {name = 'key', type = 'string'},
        {name = 'value', type = 'varbinary', is_nullable = true}
    },
    if_not_exists = true
})

vk:create_index('pk', {
    parts = {'key'},
    if_not_exists = true
})


function count_records()
    return box.space.vk:len()
end

function range(from, to)
   local result = {}
       for _, tuple in box.space.vk:pairs(from, {iterator = 'GE'}):take_while(function(x)
           return x[1] <= to  -- Assuming the `key` field is the first field
       end) do
           table.insert(result, {key = tuple[1], value = tuple[2]}) -- Assuming the `value` field is the second field
       end

   return result
end