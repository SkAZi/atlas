defmodule Atlas.Adapters.MySQL do
  @behaviour Atlas.Database.Adapter
  import Atlas.Query.Builder, only: [list_to_binding_placeholders: 1]
  import Atlas.Database.FieldNormalizer
  alias :emysql, as: MySQL

  require Record
  Record.defrecord :result_packet, Record.extract(:result_packet, from_lib: "emysql/include/emysql.hrl")
  Record.defrecord :field, Record.extract(:field, from_lib: "emysql/include/emysql.hrl")
  Record.defrecord :ok_packet, Record.extract(:ok_packet, from_lib: "emysql/include/emysql.hrl")
  Record.defrecord :error_packet, Record.extract(:error_packet, from_lib: "emysql/include/emysql.hrl")


  def connect(config) do
    args = [
        size: config.pool, 
        host: String.to_char_list(config.host), 
        database: String.to_char_list(config.database), 
        user: String.to_char_list(config.username),
        password: String.to_char_list(config.password)
    ]

    case MySQL.add_pool(args[:pool] || :mp, args) do
      {:ok, pid}       -> {:ok, pid}
      {:error, reason} -> {:error, reason}
    end
  end

  def execute_query(pid, string) do
    normalize_results(MySQL.execute(pid, string))
  end

  @doc """
  Executes prepared query with adapter after converting Atlas bindings to native formats

  Returns "normalized" results with Elixir specific types coerced from DB binaries
  """
  def execute_prepared_query(pid, query_string, args) do
    args = denormalize_values(args)
    MySQL.prepare(:native_bindings, query_string)
    MySQL.execute(pid, :native_bindings, List.flatten(args)) |> normalize_results
  end

  defp normalize_results(results) do
    case results do
      ok_packet(affected_rows: count) ->
          {:ok, {count, [], []}}

      result_packet(rows: rows, field_list: cols) ->
          cols = normalize_cols(cols)
          {:ok, {nil, cols, normalize_rows(rows, cols)}}

      error_packet(msg: error) -> 
          {:error, error}
    end
  end

  def quote_column(column), do: "`#{column}`"

  def quote_tablename(tablename), do: "`#{tablename}`"

  def quote_namespaced_column(table, column) do
    if table do
      "`#{quote_tablename(table)}`.`#{quote_column(column)}`"
    else
      quote_column(column)
    end
  end

  defp normalize_cols(columns) do
    Enum.map columns, fn field(name: name) -> String.to_atom(name) end
  end

  defp normalize_rows(rows, columns) do
    Enum.zip(columns, rows)
  end
end
