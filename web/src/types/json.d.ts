// resolveJsonModule=false のため、JSON インポートをワイルドカード宣言で通す
// eslint-disable-next-line @typescript-eslint/no-explicit-any
declare module '*.json' {
  const value: any
  export default value
}
