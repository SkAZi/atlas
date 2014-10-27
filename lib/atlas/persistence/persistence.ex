defmodule Atlas.Persistence do
  alias Atlas.Query
  alias Atlas.Database.Client

  defmacro __using__(_options) do
    quote do
      import unquote(__MODULE__)
      @before_compile unquote(__MODULE__)
    end
  end

  defmacro __before_compile__(_env) do
    quote do

      def persisted?(record, model) do
        model.primary_key_value(record) != nil
      end

      @doc """
      Updates the record within the `model.table` database table running model validations.
      Keyword list of attributes can be provided corresponding to named schema fields

      Behavior callbacks must be specified via the `as:` option to run model validations against.
      To skip behavior callbacks, simply provide an empty options list.

      Examples

        iex> user = Repo.first User
        iex> Repo.update(user, [age: 10], as: User)
        {:ok, %User{age: 10..}}

        iex> Repo.update(user, [age: 0], as: [User, Employee])
        {:error, %User{age: 0..}, ["age must be between 1 and 150"]}

      """
      def update(record, options) do
        update(record, [], options)
      end
      def update(record, attributes, options) do
        model     = Keyword.get(options, :model, record.model)
        behaviors = [Keyword.get(options, :as, record.model)] |> List.flatten
        unless Enum.member?(behaviors, model), do: behaviors = [model | behaviors]
        if Enum.any?(attributes), do: record = struct(record, attributes)

        case process_behaviors(record, behaviors) do
          {:ok, record } ->
            {sql, args} = to_prepared_update_sql(record, model)
            {:ok, _} = Client.execute_prepared_query(sql, args, __MODULE__)
            {:ok, record}

          {:error, record, reasons} -> {:error, record, reasons}
        end
      end

      # Returns {:ok, record, []} | {:error, record, error_messages}
      defp process_behaviors(record, behaviors) do
        case process_validations(record, behaviors) do
          {record, []}     -> {:ok, record}
          {record, errors} -> {:error, record, errors}
        end
      end

      defp process_validations(record, behaviors) do
        Enum.reduce behaviors, {record, []}, fn behavior, {record, errors_acc} ->
          case behavior.validate(record) do
            {:ok, record} -> {record, errors_acc}
            {:error, record, reasons} -> {record, reasons ++ errors_acc}
          end
        end
      end

      # # TODO: only update changed records - Dirty tracking?
      # def save(record) do
      #   if persisted? record do
      #     update(record, to_list(record))
      #   else
      #     create(record)
      #   end
      # end

      @doc """
      Inserts a new record in Repo's database into `model.table` provided Keyword list of attributes
      or `model` instance and run model's validations.

      Behavior callbacks must be specified via the `as:` option to run model validations against.
      To skip behavior callbacks, simply provide an empty options list.

      Examples

        iex> Repo.create(User, [age: 12], as: User)
        {:ok, %User{age: 12...}}

        iex> Repo.create(User, User.new(age: 18), as: [User, Employee])
        {:ok, %User{age: 18...}}

        iex> Repo.create(User, [age: 0], as: User)
        {:error, %User{age: 0..}, ["age must be between 1 and 150"]}

      """
      def create(model, attributes, options) when is_list(attributes) do
        create model, model.new(attributes), options
      end
      def create(model, record, options) when is_map(record) do
        behaviors = [Keyword.get(options, :as, model)] |> List.flatten

        case process_behaviors(record, behaviors) do
          {:ok, record} ->
            {sql, args} = to_prepared_insert_sql(record, model)
            {:ok, [[{_pkey, pkey_value}]]} = Client.execute_prepared_query(sql, args, __MODULE__)

            {:ok, struct(record, model.raw_kwlist_to_field_types([{_pkey, pkey_value}])) }

          {:error, record, reasons} ->
            {:error, record, reasons}
        end
      end

      # TODO: Add ability to destroy record in invalid state, or remove validation callbacks
      @doc """
      Deletes record from Repo's database in `model.table` matching record's primary key value.

      Behavior callbacks must be specified via the `as:` option to run model validations against.
      To skip behavior callbacks, simply provide an empty options list.

      Examples
        iex> user = User.first
        %User{id: 123}
        iex> Repo.destroy user, as: User
        {:ok, %User{id: nil}}

      """
      def destroy(record, options) do
        model     = Keyword.get(options, :model, record.model)
        behaviors = [Keyword.get(options, :as, record.model)] |> List.flatten

        case process_behaviors(record, behaviors) do
          {:ok, record} ->
            {sql, args} = to_prepared_delete_sql(record, model)
            case Client.execute_prepared_query(sql, args, __MODULE__) do
              {:ok, _ }         -> {:ok, struct(record, [{model.primary_key, nil}])}
              {:error, reasons} -> {:error, record, reasons}
            end
          {:error, record, reasons} -> {:error, record, reasons}
        end
      end

      @doc """
      Deletes all records from Repo's database in `model.table` matching records' primary key values

      Examples
        iex> trashed_users = User.where(archived: true) |> Repo.all
        [%User{id: 123}, %User{id: 124}, %User{id: 125}...]
        iex> Repo.destroy_all trashed_users, [User]
        {:ok, []}
      """
      def destroy_all(records) when is_list(records) do
        destroy_all(records, List.first(records).model)
      end
      def destroy_all(records, model) when is_list(records) do
        ids = Enum.map records, &model.primary_key_value(&1)
        destroy_all(model.where([{model.primary_key, ids}]))
      end
      def destroy_all(%Query{} = query) do
        {sql, args} = to_prepared_delete_sql(query, query.model)
        {:ok, _} = Client.execute_prepared_query(sql, args, __MODULE__)
      end

      defp attributes_without_nil_primary_key(record, model) do
        if model.primary_key_value(record) do
          model.to_list(record)
        else
          record
          |> model.to_list
          |> Keyword.delete_first(model.primary_key)
        end
      end

      defp to_set_sql(attributes) do
        attributes
        |> Keyword.keys
        |> Enum.map(fn column -> "#{adapter.quote_column(column)} = ?" end)
        |> Enum.join(", ")
      end

      defp to_column_sql(attributes) do
        attributes
        |> Keyword.keys
        |> Enum.map(fn column -> "#{adapter.quote_column(column)}" end)
        |> Enum.join(", ")
      end

      defp to_prepared_update_sql(record, model) do
        attributes = model.to_list(record)
        prepared_sql = """
        UPDATE #{adapter.quote_tablename(model.table)}
        SET #{to_set_sql(attributes)}
        WHERE #{adapter.quote_tablename(model.table)}.#{adapter.quote_column(model.primary_key)} = ?
        """

        { prepared_sql, Keyword.values(attributes) ++ [model.primary_key_value(record)] }
      end

      defp to_prepared_insert_sql(record, model) do
        attributes = attributes_without_nil_primary_key(record, model)

        prepared_sql = apply(adapter, :insert_sql, [model, to_column_sql(attributes)])
        { prepared_sql, [Keyword.values(attributes)] }
      end

      defp to_prepared_delete_sql(query = %Query{}, model) do
        ids = query |> all |> Enum.map(&model.primary_key_value(&1))

        prepared_sql = """
        DELETE FROM #{adapter.quote_tablename(model.table)}
        WHERE #{adapter.quote_tablename(model.table)}.#{adapter.quote_column(model.primary_key)}
        IN(?)
        """

        {prepared_sql, [ids]}
      end
      defp to_prepared_delete_sql(record, model) do
        prepared_sql = """
        DELETE FROM #{adapter.quote_tablename(model.table)}
        WHERE #{adapter.quote_tablename(model.table)}.#{adapter.quote_column(model.primary_key)} = ?
        """

        {prepared_sql, [model.primary_key_value(record)]}
      end
    end
  end
end
