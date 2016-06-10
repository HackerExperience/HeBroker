defmodule HeBroker.Mixfile do

  use Mix.Project

  def project do
    [
      app: :he_broker,
      version: "0.0.1",
      elixir: "~> 1.2",
      build_embedded: Mix.env == :prod,
      start_permanent: Mix.env == :prod,
      deps: deps]
  end

  def application do
    []
  end

  defp deps do
    [
      {:earmark, "~> 0.1", only: :dev},
      {:ex_doc, "~> 0.11", only: :dev},
      {:credo, "~> 0.4.1", only: :dev}]
  end

  defp package do
    []
  end
end
